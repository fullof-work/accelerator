package ec

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
)

func TestProbePeerRequiresWireService(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			address := "127.0.0.1:0"
			if network == "unix" {
				address = filepath.Join(t.TempDir(), "peer.sock")
			}
			lis, err := net.Listen(network, address)
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			endpoint := lis.Addr().String()
			if network == "unix" {
				endpoint = "unix://" + endpoint
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			if err := ProbePeer(ctx, endpoint); err == nil {
				t.Fatal("listener without a service passed probe")
			}
			cancel()
			// The unchanged product wire server must satisfy the strict response contract.
			srv := server.NewWireServer(server.NewCacheHandler(nil, nil), time.Second, time.Second)
			done := make(chan error, 1)
			go func() { done <- srv.Serve(lis) }()
			defer func() {
				srv.GracefulStop()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			if err := ProbePeer(context.Background(), endpoint); err != nil {
				t.Fatalf("actual wire server: %v", err)
			}
		})
	}
}
