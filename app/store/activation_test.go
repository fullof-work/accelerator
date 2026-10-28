//go:build linux

package store

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestRunExplicitFDsServeAndRemainCallerOwned(t *testing.T) {
	primary, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	path := filepath.Join(t.TempDir(), "cache.sock")
	secondary, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer secondary.Close()
	pf, err := primary.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	wf, err := secondary.(*net.UnixListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()
	cfg := runTestConfig(t, "fd=primary")
	cfg.CacheListen = "fd=cache"
	opts := Options{ListenerFDs: map[string]*os.File{"primary": pf, "cache": wf}, Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"G1"}, nil }}}
	// Even a conflicting process environment must not affect the explicit SDK.
	t.Setenv("LISTEN_PID", "invalid")
	t.Setenv("LISTEN_FDS", "999")
	t.Setenv("LISTEN_FDNAMES", "unused")
	for i := 0; i < 2; i++ {
		cancel, done := startRunTest(t, cfg, opts)
		conn := waitForRunTestStore(t, primary.Addr().String())
		requireRunTestRPC(t, conn, "G1")
		conn.Close()
		raw, err := net.DialTimeout("unix", path, runTestTimeout)
		if err != nil {
			t.Fatal(err)
		}
		raw.SetDeadline(time.Now().Add(runTestTimeout))
		if err := wire.WriteRequest(raw, &wire.Request{Opcode: wire.OpcodePing}); err != nil {
			raw.Close()
			t.Fatal(err)
		}
		resp, err := wire.ReadResponse(raw, nil)
		raw.Close()
		if err != nil || resp.Status != wire.StatusHit || resp.Value != nil || resp.ErrMsg != "" {
			t.Fatalf("wire Ping=%v, %v", resp, err)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, err := pf.Stat(); err != nil {
			t.Fatalf("original primary closed: %v", err)
		}
		if _, err := wf.Stat(); err != nil {
			t.Fatalf("original cache closed: %v", err)
		}
		if st, err := os.Stat(path); err != nil || st.Mode()&os.ModeSocket == 0 {
			t.Fatalf("manager UDS removed: %v", err)
		}
	}
}

func TestRunInvalidFDsRejectBeforeSourceInitialization(t *testing.T) {
	cfg := runTestConfig(t, "fd=missing")
	err := Run(context.Background(), cfg, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
		t.Fatal("source initialized before FD validation")
		return nil, nil
	}}})
	if err == nil {
		t.Fatal("missing FD accepted")
	}
}
