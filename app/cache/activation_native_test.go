//go:build !no_rocksdb

package cache

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

type activationStartupLog struct {
	started chan struct{}
	once    sync.Once
	warned  atomic.Bool
}

func (w *activationStartupLog) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("cache pprof disabled")) {
		w.warned.Store(true)
	}
	if bytes.Contains(p, []byte("cache started")) {
		w.once.Do(func() { close(w.started) })
	}
	return len(p), nil
}

func TestOrdinaryPprofBindFailureStillServes(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	cfg := Config{Mode: "local", Type: "embedded", Listen: drainTestAddress(t), PprofListen: occupied.Addr().String(), StatsInterval: "off", Rocks: *embeddedTier(t).Rocks, Freq: runtime.FreqConfig{Counters: "1M", ResetAfter: "100K"}}
	log := &activationStartupLog{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done, joined := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(joined)
		done <- Run(ctx, cfg, Options{Logger: slog.New(slog.NewTextHandler(log, nil))})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Run did not join")
		}
	})
	select {
	case <-log.started:
	case err := <-done:
		t.Fatalf("ordinary pprof bind failure stopped App: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("App did not finish initialization")
	}
	if !log.warned.Load() {
		t.Fatal("ordinary pprof bind error was not diagnosed")
	}
	probe, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := client.Ping(probe, cfg.Listen); err != nil {
		t.Fatalf("data protocol unavailable after optional bind failure: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("App did not stop")
	}
}
