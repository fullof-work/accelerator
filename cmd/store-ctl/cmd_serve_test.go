package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	appstore "github.com/kuasar-sandbox/accelerator/app/store"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServeStoreTerminationIsIndependentOfSaturatedReload(t *testing.T) {
	term, hup := make(chan os.Signal, 1), make(chan os.Signal, 8)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serveStore(Config{}, "", term, hup, &bytes.Buffer{}, 0, func(ctx context.Context, _ appstore.Config, opts appstore.Options) error {
			close(started)
			<-opts.Drain
			if ctx.Err() != nil {
				return errors.New("first signal cancelled Run context")
			}
			return nil
		})
	}()
	<-started
	for range 8 {
		hup <- syscall.SIGHUP
	}
	term <- syscall.SIGTERM
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("termination was blocked by saturated reload notifications")
	}
}

func TestServeStoreEarlyRunExitJoinsForwarder(t *testing.T) {
	term, hup := make(chan os.Signal), make(chan os.Signal)
	want := errors.New("run failed")
	if err := serveStore(Config{}, "", term, hup, &bytes.Buffer{}, 0, func(context.Context, appstore.Config, appstore.Options) error { return want }); !errors.Is(err, want) {
		t.Fatalf("serveStore error = %v", err)
	}
}

type reloadFailureWriter chan string

func (w reloadFailureWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "generation refresh failed") {
		select {
		case w <- string(p):
		default:
		}
	}
	return len(p), nil
}

func TestServeStoreInlineAdapterInitialReloadAndRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.yaml")
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	write := func(gens string) {
		writeText(t, path+".next", fmt.Sprintf("listen: %s\nbackend: fs\nfs:\n  root: %s\ngenerations:\n  config:\n%s", addr, root, gens))
		if err := os.Rename(path+".next", path); err != nil {
			t.Fatal(err)
		}
	}
	write("    - G1\n")
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatal(err)
	}
	term, hup := make(chan os.Signal, 1), make(chan os.Signal, 1)
	done, stopped := make(chan error, 1), make(chan struct{})
	failures := make(reloadFailureWriter, 8)
	go func() {
		defer close(stopped)
		done <- serveStore(*cfg, path, term, hup, &bytes.Buffer{}, 0, func(ctx context.Context, cfg appstore.Config, opts appstore.Options) error {
			if opts.Generations == nil || opts.Generations.RefreshInterval != 0 {
				return errors.New("inline YAML adapter missing or unexpectedly periodic")
			}
			opts.Logger = slog.New(slog.NewTextHandler(failures, nil))
			return appstore.Run(ctx, cfg, opts)
		})
	}()
	t.Cleanup(func() {
		select {
		case term <- syscall.SIGTERM:
		default:
		}
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("CLI App cleanup did not finish")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, cfg.Listen, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewStoreClient(conn)
	wantGeneration := func(want string) {
		t.Helper()
		for {
			response, rpcErr := client.AdmitWrite(ctx, &pb.AdmitWriteRequest{})
			if rpcErr == nil && response.GetGeneration() == want {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("generation=%v error=%v, want %s", response, rpcErr, want)
			case <-time.After(time.Millisecond):
			}
		}
	}
	waitFailure := func(fragment string) {
		t.Helper()
		select {
		case msg := <-failures:
			if !strings.Contains(msg, fragment) {
				t.Fatalf("unexpected refresh failure: %s", msg)
			}
		case <-ctx.Done():
			t.Fatal("invalid reload was not processed")
		}
	}
	wantGeneration("G1")
	write("    - G1\n    - G2\n")
	hup <- syscall.SIGHUP
	wantGeneration("G2")
	write("    - G1\n    - G1\n")
	hup <- syscall.SIGHUP
	waitFailure("duplicate")
	wantGeneration("G2")
	writeText(t, path, fmt.Sprintf("listen: %s\nbackend: fs\nfs:\n  root: %s\ngenerations:\n  file:\n    path: %s\n", addr, root, filepath.Join(root, "unused")))
	hup <- syscall.SIGHUP
	waitFailure("changed type")
	wantGeneration("G2")
	term <- syscall.SIGTERM
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("CLI did not finish")
	}
}

func TestServeStoreLeavesFileAndS3SourcesToApp(t *testing.T) {
	for name, generations := range map[string]*storeconfig.GenerationsConfig{
		"file": {File: &storeconfig.GenerationFileConfig{Path: "/unused"}, RefreshInterval: "5s"},
		"s3":   {S3: &storeconfig.GenerationS3Config{Endpoint: "http://example", Bucket: "b", Key: "k"}, RefreshInterval: "5s"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Generations: generations}
			err := serveStore(cfg, "ignored", make(chan os.Signal), make(chan os.Signal), &bytes.Buffer{}, 0, func(_ context.Context, _ appstore.Config, opts appstore.Options) error {
				if opts.Generations != nil {
					t.Fatal("CLI replaced built-in App source")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeStoreSecondSignalCancelsAndIgnoresLaterSignals(t *testing.T) {
	term, hup := make(chan os.Signal, 8), make(chan os.Signal, 8)
	first, second, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serveStore(Config{}, "", term, hup, &bytes.Buffer{}, time.Second, func(ctx context.Context, _ appstore.Config, opts appstore.Options) error {
			if opts.DrainTimeout != time.Second {
				return errors.New("lost CLI drain timeout")
			}
			<-opts.Drain
			if ctx.Err() != nil {
				return errors.New("first signal cancelled work")
			}
			close(first)
			<-ctx.Done()
			close(second)
			<-release
			return nil
		})
	}()
	term <- syscall.SIGTERM
	<-first
	hup <- syscall.SIGHUP
	term <- syscall.SIGINT
	<-second
	term <- syscall.SIGTERM
	term <- syscall.SIGINT
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
