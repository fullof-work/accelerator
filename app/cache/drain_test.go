//go:build !no_rocksdb

package cache

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func drainTestAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestDrainJoinsPprofHandlerAndEscalatesShutdown(t *testing.T) {
	for _, kind := range []string{"graceful", "context", "budget"} {
		t.Run(kind, func(t *testing.T) {
			cfg := Config{Mode: "local", Type: "embedded", Listen: drainTestAddress(t), PprofListen: drainTestAddress(t), StatsInterval: "off", Rocks: *embeddedTier(t).Rocks, Freq: runtime.FreqConfig{Counters: "1M", ResetAfter: "100K"}}
			ctx, cancel := context.WithCancel(context.Background())
			drain := make(chan struct{})
			opts := Options{Drain: drain}
			if kind == "budget" {
				opts.DrainTimeout = 20 * time.Millisecond
			}
			done, joined := make(chan error, 1), make(chan struct{})
			go func() { defer close(joined); done <- Run(ctx, cfg, opts) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("Run cleanup did not join")
				}
			})
			var c net.Conn
			until := time.Now().Add(5 * time.Second)
			for time.Now().Before(until) {
				var err error
				c, err = net.DialTimeout("tcp", cfg.PprofListen, time.Second)
				if err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("Run startup: %v", err)
				case <-time.After(time.Millisecond):
				}
			}
			if c == nil {
				t.Fatal("pprof did not listen")
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.WriteString(c, "POST /debug/pprof/symbol HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(c)
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 100 {
				t.Fatalf("handler admission: %s", resp.Status)
			}
			resp.Body.Close()
			// 100 Continue proves the actual pprof handler started reading its
			// body. It remains an active resource user until body completion/close.
			close(drain)
			if kind == "graceful" {
				// Observe the App's actual drain, not merely the caller's channel
				// close, before allowing the admitted handler to finish its body.
				until := time.Now().Add(time.Second)
				for {
					probe, err := net.DialTimeout("tcp", cfg.PprofListen, time.Second)
					if err != nil {
						break
					}
					probe.Close()
					if time.Now().After(until) {
						t.Fatal("drain did not close pprof listener")
					}
					time.Sleep(time.Millisecond)
				}
				select {
				case err := <-done:
					t.Fatalf("abandoned HTTP handler: %v", err)
				default:
				}
				if _, err := io.WriteString(c, strings.Repeat("0", 100)); err != nil {
					t.Fatal(err)
				}
				resp, err := http.ReadResponse(reader, nil)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != 200 {
					t.Fatalf("drained HTTP response: %s", resp.Status)
				}
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
			} else if kind == "context" {
				cancel()
			}
			select {
			case err := <-done:
				if kind == "budget" {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("budget result=%v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown did not cancel/join active pprof handler")
			}
			if kind != "graceful" {
				if _, err := http.ReadResponse(reader, nil); err == nil {
					t.Fatal("incomplete pprof request reported success")
				}
			}
		})
	}
}
