package driver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	volumeBasePath   = "/var/lib/csi-driver"
	topologyKey      = "kubernetes.io/hostname"
	deleteJobNS      = "kube-system"
	deleteJobTimeout = 2 * time.Minute
)

var errNodeNotFound = errors.New("node not found")
var errNoNodeAffinity = errors.New("no nodeAffinity on PV")

type ControllerServer struct {
	csi.UnimplementedControllerServer
	kubeClient kubernetes.Interface
}

func (s *ControllerServer) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	// Preferred → Requisite の順でトポロジーを選ぶ。
	// external-provisioner が WaitForFirstConsumer で決定したノードに PV を固定
	var topology []*csi.Topology
	if topo := req.GetAccessibilityRequirements(); topo != nil {
		if len(topo.GetPreferred()) > 0 {
			topology = []*csi.Topology{topo.GetPreferred()[0]}
		} else if len(topo.GetRequisite()) > 0 {
			topology = []*csi.Topology{topo.GetRequisite()[0]}
		}
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           req.GetName(),
			CapacityBytes:      req.GetCapacityRange().GetRequiredBytes(),
			AccessibleTopology: topology,
		},
	}, nil
}

func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	nodeName, err := s.nodeFromPV(ctx, req.GetVolumeId())
	if err != nil {
		if errors.Is(err, errNodeNotFound) {
			// PV が既に消えていれば削除済みとみなして成功を返す（冪等性）
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to find node for volume: %v", err)
	}

	if err := s.runDeleteJob(ctx, req.GetVolumeId(), nodeName); err != nil {
		return nil, status.Errorf(codes.Internal, "delete job failed: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// PV の nodeAffinity から対象ノード名を取得
func (s *ControllerServer) nodeFromPV(ctx context.Context, volumeID string) (string, error) {
	pvList, err := s.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, pv := range pvList.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle != volumeID {
			continue
		}
		// PV は存在するが nodeAffinity がない場合はノードを特定できない。
		// 削除済みとはみなせないため errNoNodeAffinity を返す。
		if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
			return "", errNoNodeAffinity
		}
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == topologyKey && len(expr.Values) > 0 {
					return expr.Values[0], nil
				}
			}
		}
		return "", errNoNodeAffinity
	}

	return "", errNodeNotFound
}

// は対象ノードで `rm -rf <volumeBasePath>/<volumeID>` を実行する Job を作成し、完了を待つ。
func (s *ControllerServer) runDeleteJob(ctx context.Context, volumeID, nodeName string) error {
	jobName := deleteJobName(volumeID)

	privileged := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: deleteJobNS,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeName:      nodeName,
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "del",
							Image:   "busybox:1.36",
							Command: []string{"rm", "-rf", fmt.Sprintf("%s/%s", volumeBasePath, volumeID)},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "host", MountPath: volumeBasePath},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: volumeBasePath},
							},
						},
					},
				},
			},
		},
	}

	_, err := s.kubeClient.BatchV1().Jobs(deleteJobNS).Create(ctx, job, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}

	deadline := time.Now().Add(deleteJobTimeout)
	for time.Now().Before(deadline) {
		j, err := s.kubeClient.BatchV1().Jobs(deleteJobNS).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if j.Status.Succeeded > 0 {
			_ = s.kubeClient.BatchV1().Jobs(deleteJobNS).Delete(ctx, jobName, metav1.DeleteOptions{})
			return nil
		}
		if j.Status.Failed > 3 {
			_ = s.kubeClient.BatchV1().Jobs(deleteJobNS).Delete(ctx, jobName, metav1.DeleteOptions{})
			return fmt.Errorf("delete job failed after retries")
		}
		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("delete job timed out after %v", deleteJobTimeout)
}

// volumeID から 63 文字以内の一意な Job 名を生成
func deleteJobName(volumeID string) string {
	const prefix = "csi-del-"
	name := prefix + volumeID
	if len(name) <= 63 {
		return name
	}
	h := sha256.Sum256([]byte(volumeID))
	return fmt.Sprintf("%s%x", prefix, h[:])[:63]
}

func (s *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
		},
	}, nil
}
