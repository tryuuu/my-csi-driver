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
	createJobTimeout = 5 * time.Minute
)

var errNodeNotFound = errors.New("node not found")
var errNoNodeAffinity = errors.New("no nodeAffinity on PV")

type ControllerServer struct {
	csi.UnimplementedControllerServer
	kubeClient kubernetes.Interface
	jobImage   string
}

func (s *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	sizeBytes := req.GetCapacityRange().GetRequiredBytes()
	if sizeBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "required bytes must be greater than 0")
	}

	// Preferred → Requisite の順でトポロジーを選ぶ。
	// external-provisioner が WaitForFirstConsumer で決定したノードに PV を固定
	var topology []*csi.Topology
	var nodeName string
	if topo := req.GetAccessibilityRequirements(); topo != nil {
		if len(topo.GetPreferred()) > 0 {
			topology = []*csi.Topology{topo.GetPreferred()[0]}
			nodeName = topo.GetPreferred()[0].GetSegments()[topologyKey]
		} else if len(topo.GetRequisite()) > 0 {
			topology = []*csi.Topology{topo.GetRequisite()[0]}
			nodeName = topo.GetRequisite()[0].GetSegments()[topologyKey]
		}
	}
	if nodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "cannot determine target node from topology")
	}

	if err := s.runCreateJob(ctx, req.GetName(), nodeName, sizeBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "create job failed: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           req.GetName(),
			CapacityBytes:      sizeBytes,
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

// 対象ノードでイメージファイルの作成・ループデバイス割り当て・フォーマットを行う Job を実行する。
func (s *ControllerServer) runCreateJob(ctx context.Context, volumeID, nodeName string, sizeBytes int64) error {
	jobName := createJobName(volumeID)
	imgPath := fmt.Sprintf("%s/%s.img", volumeBasePath, volumeID) // /var/lib/csi-driver/<id>.img

	script := fmt.Sprintf(`set -e
IMG='%s'
if [ ! -f "$IMG" ]; then
  fallocate -l %d "$IMG"
fi
ATTACHED=0
DEV=$(losetup -j "$IMG" | cut -d: -f1)
if [ -z "$DEV" ]; then
  DEV=$(losetup -f --show "$IMG")
  ATTACHED=1
fi
if ! blkid "$DEV" >/dev/null 2>&1; then
  mkfs.ext4 "$DEV"
fi
if [ "$ATTACHED" = 1 ]; then
  losetup -d "$DEV"
fi`, imgPath, sizeBytes)

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
							Name:    "create",
							Image:   s.jobImage,
							Command: []string{"/bin/sh", "-c", script},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: volumeBasePath},
								{Name: "dev", MountPath: "/dev"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								// HostPath: Node
								HostPath: &corev1.HostPathVolumeSource{Path: volumeBasePath}, // Node: /var/lib/csi-driver/
							},
						},
						{
							Name: "dev",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/dev"}, // Node: /dev/loopN/
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

	deadline := time.Now().Add(createJobTimeout)
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
			return fmt.Errorf("create job failed after retries")
		}
		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("create job timed out after %v", createJobTimeout)
}

func createJobName(volumeID string) string {
	const prefix = "csi-create-"
	name := prefix + volumeID
	if len(name) <= 63 {
		return name
	}
	h := sha256.Sum256([]byte(volumeID))
	return fmt.Sprintf("%s%x", prefix, h[:])[:63]
}

// 対象ノードでループデバイスのデタッチとイメージファイルの削除を行う Job を作成し、完了を待つ。
func (s *ControllerServer) runDeleteJob(ctx context.Context, volumeID, nodeName string) error {
	jobName := deleteJobName(volumeID)
	imgPath := fmt.Sprintf("%s/%s.img", volumeBasePath, volumeID)

	script := fmt.Sprintf(`set -e
IMG='%s'
DEV=$(losetup -j "$IMG" | cut -d: -f1)
if [ -n "$DEV" ]; then
  umount "$DEV" 2>/dev/null || true
  losetup -d "$DEV"
fi
rm -f "$IMG"
rm -rf '%s/%s'`, imgPath, volumeBasePath, volumeID)

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
							Name:    "delete",
							Image:   s.jobImage,
							Command: []string{"/bin/sh", "-c", script},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: volumeBasePath},
								{Name: "dev", MountPath: "/dev"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: volumeBasePath},
							},
						},
						{
							Name: "dev",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/dev"},
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
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (s *ControllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	capacityBytes, err := capacityBytesFromRange(req.GetCapacityRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid capacity range: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         capacityBytes,
		NodeExpansionRequired: true,
	}, nil
}

func capacityBytesFromRange(cr *csi.CapacityRange) (int64, error) {
	if cr == nil {
		return 0, fmt.Errorf("capacity range is required")
	}

	required := cr.GetRequiredBytes()
	if required <= 0 {
		return 0, fmt.Errorf("required bytes must be greater than 0")
	}
	return required, nil
}
