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
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volume is not supported")
	}

	stagingPath := req.GetStagingTargetPath()
	stagingNotMnt, err := s.mounter.IsLikelyNotMountPoint(stagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "staging target path is not mounted: %s", stagingPath)
		}
		return nil, status.Errorf(codes.Internal, "failed to check staging mount point: %v", err)
	}
	if stagingNotMnt {
		return nil, status.Errorf(codes.FailedPrecondition, "staging target path is not mounted: %s", stagingPath)
	}

	targetPath := req.GetTargetPath()
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target path: %v", err)
	}

	notMnt, err := s.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := s.mounter.Mount(stagingPath, targetPath, "", []string{"bind"}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount: %v", err)
	}

	// Linux の bind mount では1回の mount で ro を有効にできない。
	// bind 後に remount,bind,ro で読み取り専用を適用する。
	if req.GetReadonly() {
		if err := s.mounter.Mount(stagingPath, targetPath, "", []string{"bind", "remount", "ro"}); err != nil {
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

func (s *NodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volume is not supported")
	}

	stagingPath := req.GetStagingTargetPath()

	notMnt, err := s.mounter.IsLikelyNotMountPoint(stagingPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
	}
	if !notMnt {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	imgPath := fmt.Sprintf("%s/%s.img", volumeBasePath, req.GetVolumeId())
	dev, err := losetupAttach(imgPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "losetup attach failed: %v", err)
	}

	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging path: %v", err)
	}

	if err := s.mounter.Mount(dev, stagingPath, "ext4", []string{}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount loop device: %v", err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *NodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	if err := mount.CleanupMountPoint(req.GetStagingTargetPath(), s.mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount staging path: %v", err)
	}

	imgPath := fmt.Sprintf("%s/%s.img", volumeBasePath, req.GetVolumeId())
	if err := losetupDetach(imgPath); err != nil {
		return nil, status.Errorf(codes.Internal, "losetup detach failed: %v", err)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (s *NodeServer) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volume is not supported")
	}

	capacityBytes, err := capacityBytesFromRange(req.GetCapacityRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid capacity range: %v", err)
	}

	imgPath := fmt.Sprintf("%s/%s.img", volumeBasePath, req.GetVolumeId())
	currentSize, err := imageSizeBytes(imgPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stat image file: %v", err)
	}

	// 拡張のみ対応
	finalSize := currentSize
	if capacityBytes > currentSize {
		if err := exec.Command("truncate", "-s", strconv.FormatInt(capacityBytes, 10), imgPath).Run(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resize image file: %v", err)
		}
		finalSize = capacityBytes
	}

	dev, err := losetupAttach(imgPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "losetup attach failed: %v", err)
	}

	if out, err := exec.Command("losetup", "-c", dev).CombinedOutput(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resize loop device: %v: %s", err, out)
	}
	if out, err := exec.Command("resize2fs", dev).CombinedOutput(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resize ext4 filesystem: %v: %s", err, out)
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: finalSize,
	}, nil
}

func imageSizeBytes(path string) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func losetupAttach(imgPath string) (string, error) {
	out, err := exec.Command("losetup", "-j", imgPath).Output()
	if err == nil && len(strings.TrimSpace(string(out))) > 0 {
		return strings.SplitN(strings.TrimSpace(string(out)), ":", 2)[0], nil
	}
	out, err = exec.Command("losetup", "-f", "--show", imgPath).Output()
	if err != nil {
		return "", fmt.Errorf("losetup -f --show: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func losetupDetach(imgPath string) error {
	out, err := exec.Command("losetup", "-j", imgPath).Output()
	if err != nil || len(strings.TrimSpace(string(out))) == 0 {
		return nil
	}
	dev := strings.SplitN(strings.TrimSpace(string(out)), ":", 2)[0]
	if out, err := exec.Command("losetup", "-d", dev).CombinedOutput(); err != nil {
		return fmt.Errorf("losetup -d %s: %w: %s", dev, err, out)
	}
	return nil
}

func (s *NodeServer) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	// volumePath にはボリューム専用の ext4（loop デバイス）がマウントされているため、
	// statfs の結果がそのまま PVC の容量・使用量になる。
	var stat syscall.Statfs_t
	if err := syscall.Statfs(req.GetVolumePath(), &stat); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s not found", req.GetVolumePath())
		}
		return nil, status.Errorf(codes.Internal, "failed to statfs %s: %v", req.GetVolumePath(), err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Total:     int64(stat.Blocks) * int64(stat.Bsize),
				Used:      int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize),
				Available: int64(stat.Bavail) * int64(stat.Bsize),
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Total:     int64(stat.Files),
				Used:      int64(stat.Files) - int64(stat.Ffree),
				Available: int64(stat.Ffree),
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
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
