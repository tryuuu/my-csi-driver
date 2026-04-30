package driver

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/mount-utils"
)

type NodeServer struct {
	csi.UnimplementedNodeServer
	nodeID   string
	hostname string
	mounter  mount.Interface
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
		nodeID:   nodeID,
		hostname: hostname,
		mounter:  mount.New(""),
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
	return &csi.NodeGetCapabilitiesResponse{}, nil
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
