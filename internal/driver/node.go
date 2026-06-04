package driver

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/mount-utils"
)

type NodeServer struct {
	csi.UnimplementedNodeServer
	nodeID     string
	hostname   string
	mounter    mount.Interface
	kubeClient kubernetes.Interface
}

func NewNodeServer(kubeClient kubernetes.Interface) (*NodeServer, error) {
	nodeID := os.Getenv("NODE_NAME")

	// Node オブジェクトから実際のラベル値を取得
	hostname := nodeID
	if nodeID != "" {
		node, err := kubeClient.CoreV1().Nodes().Get(context.Background(), nodeID, metav1.GetOptions{})
		if err == nil {
			if v := node.Labels[topologyKey]; v != "" {
				hostname = v
			}
		}
	}

	return &NodeServer{
		nodeID:     nodeID,
		hostname:   hostname,
		mounter:    mount.New(""),
		kubeClient: kubeClient,
	}, nil
}

func (s *NodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	// このドライバはFilesystemモードのみ対応。Blockは拒否する。
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volume is not supported")
	}

	dataDir := volumeBasePath + "/" + req.GetVolumeId()
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create data dir: %v", err)
	}
	// MkdirAll respects the process umask, so explicitly set permissions.
	if err := os.Chmod(dataDir, 0777); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to chmod data dir: %v", err)
	}

	targetPath := req.GetTargetPath()
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target path: %v", err)
	}

	// 既にマウント済みであればスキップ（冪等性）
	notMnt, err := s.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := s.mounter.Mount(dataDir, targetPath, "", []string{"bind"}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount: %v", err)
	}

	// Linux の bind mount では1回の mount で ro を有効にできない。
	// bind 後に remount,bind,ro で読み取り専用を適用する。
	if req.GetReadonly() {
		if err := s.mounter.Mount(dataDir, targetPath, "", []string{"bind", "remount", "ro"}); err != nil {
			// remount 失敗時は RW マウントを残さないよう解除してからエラーを返す。
			_ = mount.CleanupMountPoint(targetPath, s.mounter, true)
			return nil, status.Errorf(codes.Internal, "failed to remount read-only: %v", err)
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *NodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	if err := mount.CleanupMountPoint(req.GetTargetPath(), s.mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount: %v", err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

func (s *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(req.GetVolumePath(), &stat); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s not found", req.GetVolumePath())
		}
		return nil, status.Errorf(codes.Internal, "failed to statfs %s: %v", req.GetVolumePath(), err)
	}

	capacityBytes, err := s.pvCapacityBytes(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get PV capacity for %s: %v", req.GetVolumeId(), err)
	}

	dataDir := volumeBasePath + "/" + req.GetVolumeId()
	usedBytes, err := dirUsedBytes(dataDir)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get used bytes for %s: %v", dataDir, err)
	}
	usedInodes, err := dirUsedInodes(dataDir)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get used inodes for %s: %v", dataDir, err)
	}

	availableBytes := min(capacityBytes-usedBytes, int64(stat.Bavail)*int64(stat.Bsize))
	if availableBytes < 0 {
		availableBytes = 0
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Total:     capacityBytes,
				Used:      usedBytes,
				Available: availableBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Total:     int64(stat.Files),
				Used:      usedInodes,
				Available: int64(stat.Ffree),
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// volumeID に対応する PV の要求容量をbyteで返す
func (s *NodeServer) pvCapacityBytes(ctx context.Context, volumeID string) (int64, error) {
	pvList, err := s.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	for _, pv := range pvList.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle != volumeID {
			continue
		}
		if q, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
			return q.Value(), nil
		}
	}
	return 0, fmt.Errorf("PV with volumeHandle %q not found", volumeID)
}

// du -sb でディレクトリの使用バイト数を返す。
func dirUsedBytes(path string) (int64, error) {
	out, err := exec.Command("du", "-s", "-B1", path).Output()
	if err != nil {
		return 0, err
	}
	// du -s -B1 の出力形式: "<bytes>\t<path>\n"
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return 0, fmt.Errorf("unexpected du output: %q", string(out))
	}
	return strconv.ParseInt(fields[0], 10, 64)
}

// dirUsedInodes は du --inodes -s でディレクトリの使用inode数を返す。
func dirUsedInodes(path string) (int64, error) {
	out, err := exec.Command("du", "--inodes", "-s", path).Output()
	if err != nil {
		return 0, err
	}
	// du --inodes -s の出力形式: "<inodes>\t<path>\n"
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return 0, fmt.Errorf("unexpected du output: %q", string(out))
	}
	return strconv.ParseInt(fields[0], 10, 64)
}

func (s *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				topologyKey: s.hostname,
			},
		},
	}, nil
}
