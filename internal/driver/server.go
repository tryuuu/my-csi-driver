package driver

import (
	"net"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

func Run(socketPath string) error {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, &IdentityServer{})

	return srv.Serve(lis)
}
