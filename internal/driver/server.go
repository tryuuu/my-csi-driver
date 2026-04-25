package driver

import (
	"fmt"
	"net"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func Run(socketPath string) error {
	kubeClient, err := newKubeClient()
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, &IdentityServer{})
	csi.RegisterControllerServer(srv, &ControllerServer{kubeClient: kubeClient})

	return srv.Serve(lis)
}

// newKubeClient はクラスタ内設定を優先し、失敗時は ~/.kube/config にフォールバック
func newKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.BuildConfigFromFlags("", loadingRules.GetDefaultFilename())
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(cfg)
}
