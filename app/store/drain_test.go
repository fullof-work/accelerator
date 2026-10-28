package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

func waitPutInode(t *testing.T, ctx context.Context, cfg Config, key [32]byte) string {
	t.Helper()
	hexKey := fmt.Sprintf("%x", key)
	path := filepath.Join(cfg.FS.Root, "blob", "G1", hexKey[:2], hexKey[2:4], hexKey)
	for {
		if _, err := os.Stat(path); err == nil {
			return path
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("Put was not admitted")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestDrainJoinsBothStoreEntrancesAndEveryGetSend(t *testing.T) {
	for _, finishFirst := range []string{"grpc", "wire"} {
		t.Run(finishFirst, func(t *testing.T) {
			cfg := runTestConfig(t, runTestAddress(t))
			cfg.CacheListen = runTestAddress(t)
			data := bytes.Repeat([]byte{0x63}, 3<<20)
			key := sha256.Sum256(data)
			hexKey := fmt.Sprintf("%x", key)
			path := filepath.Join(cfg.FS.Root, "blob", "G1", hexKey[:2], hexKey[2:4], hexKey)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			drain := make(chan struct{})
			_, done := startRunTest(t, cfg, Options{Drain: drain, Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"G1"}, nil }}})
			ctx, cancel := context.WithTimeout(context.Background(), runTestTimeout)
			defer cancel()
			conn, err := grpc.DialContext(ctx, cfg.Listen, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
				grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(64<<10))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			stream, err := pb.NewStoreClient(conn).Get(ctx, &pb.GetRequest{Partition: pb.Partition_PARTITION_BLOB, Key: key[:]})
			if err != nil {
				t.Fatal(err)
			}
			first, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := net.DialTimeout("tcp", cfg.CacheListen, runTestTimeout)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if err := raw.(*net.TCPConn).SetReadBuffer(64 << 10); err != nil {
				t.Fatal(err)
			}
			raw.SetDeadline(time.Now().Add(runTestTimeout))
			if err := wire.WriteRequest(raw, &wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSBlob, Hash: key}); err != nil {
				t.Fatal(err)
			}
			header := make([]byte, wire.ResponseHeaderSize)
			if _, err := io.ReadFull(raw, header); err != nil {
				t.Fatal(err)
			}
			close(drain)
			readGRPC := func() {
				got := bytes.Clone(first.Data)
				for {
					m, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, m.Data...)
				}
				if !bytes.Equal(got, data) {
					t.Fatal("Store Get lost a streamed frame")
				}
			}
			readWire := func() {
				resp, err := wire.ReadResponse(io.MultiReader(bytes.NewReader(header), raw), nil)
				if err != nil {
					t.Fatal(err)
				}
				if resp.Value == nil {
					t.Fatal("lost cache response")
				}
				defer resp.Value.Release()
				if resp.Status != wire.StatusHit || !bytes.Equal(resp.Value.Bytes(), data) {
					t.Fatal("cache response truncated/corrupt")
				}
			}
			if finishFirst == "grpc" {
				readGRPC()
			} else {
				readWire()
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned while other entrance was active: %v", err)
			default:
			}
			if finishFirst == "grpc" {
				readWire()
			} else {
				readGRPC()
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDrainJoinsBorrowedGenerationRefresh(t *testing.T) {
	cfg := runTestConfig(t, runTestAddress(t))
	drain, reload := make(chan struct{}), make(chan struct{}, 2)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	_, done := startRunTest(t, cfg, Options{Drain: drain, Reload: reload, Generations: &GenerationSource{Load: func(ctx context.Context) ([]pkgstore.Generation, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			<-release
		}
		return []pkgstore.Generation{"G1"}, nil
	}}})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	conn := waitForRunTestStore(t, cfg.Listen)
	defer conn.Close()
	requireRunTestRPC(t, conn, "G1")
	reload <- struct{}{}
	<-entered
	close(drain)
	<-cancelled
	reload <- struct{}{}
	select {
	case err := <-done:
		t.Fatalf("Run abandoned borrowed callback: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("refresh after drain: %d", calls.Load())
	}
}

func TestDrainPutCompletesReceiveCommitAndResponse(t *testing.T) {
	cfg := runTestConfig(t, runTestAddress(t))
	drain := make(chan struct{})
	_, done := startRunTest(t, cfg, Options{Drain: drain, Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"G1"}, nil }}})
	conn := waitForRunTestStore(t, cfg.Listen)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), runTestTimeout)
	defer cancel()
	data := bytes.Repeat([]byte("drain-stream"), 64<<10)
	key := sha256.Sum256(data)
	stream, err := pb.NewStoreClient(conn).Put(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_BLOB, Key: key[:], Generation: "G1"}}}); err != nil {
		t.Fatal(err)
	}
	path := waitPutInode(t, ctx, cfg, key)
	close(drain)
	// This newly initiated operation is rejected even on the already-open HTTP/2
	// connection; the earlier stream retains its admission and context.
	if _, err := pb.NewStoreClient(conn).AdmitWrite(ctx, &pb.AdmitWriteRequest{}); err == nil {
		t.Fatal("new operation admitted after drain")
	}
	for start := 0; start < len(data); start += 32 << 10 {
		end := start + (32 << 10)
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: data[start:end]}}); err != nil {
			t.Fatalf("draining Put send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("draining Put did not receive commit result: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed data differs: %v", err)
	}
}

func TestDrainEscalationAbortsActivePut(t *testing.T) {
	for _, kind := range []string{"context", "budget"} {
		t.Run(kind, func(t *testing.T) {
			cfg := runTestConfig(t, runTestAddress(t))
			drain := make(chan struct{})
			opts := Options{Drain: drain, Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"G1"}, nil }}}
			if kind == "budget" {
				opts.DrainTimeout = 30 * time.Millisecond
			}
			stop, done := startRunTest(t, cfg, opts)
			conn := waitForRunTestStore(t, cfg.Listen)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), runTestTimeout)
			defer cancel()
			key := sha256.Sum256([]byte("never-completed"))
			stream, err := pb.NewStoreClient(conn).Put(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_BLOB, Key: key[:], Generation: "G1"}}}); err != nil {
				t.Fatal(err)
			}
			path := waitPutInode(t, ctx, cfg, key)
			close(drain)
			if kind == "context" {
				stop()
			}
			err = <-done
			if kind == "context" && err != nil {
				t.Fatal(err)
			}
			if kind == "budget" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("budget error=%v", err)
			}
			if _, err := stream.CloseAndRecv(); err == nil {
				t.Fatal("aborted Put reported success")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("aborted inode retained: %v", err)
			}
		})
	}
}

func TestDrainDuringUncancellableInitializationJoinsAndNeverPublishes(t *testing.T) {
	cfg := runTestConfig(t, runTestAddress(t))
	drain, entered, release, expired := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var log bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), cfg, Options{Drain: drain, DrainTimeout: 20 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&log, nil)), Generations: &GenerationSource{Load: func(ctx context.Context) ([]pkgstore.Generation, error) {
			close(entered)
			<-ctx.Done()
			close(expired)
			<-release
			return []pkgstore.Generation{"G1"}, nil
		}}})
	}()
	<-entered
	close(drain)
	<-expired
	select {
	case <-done:
		t.Fatal("Run abandoned active provider")
	default:
	}
	close(release)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("budget result=%v", err)
	}
	if strings.Contains(log.String(), "store started") {
		t.Fatalf("published after initialization drain: %s", log.String())
	}
}

func TestNegativeDrainBudgetDoesNotCallProvider(t *testing.T) {
	cfg := runTestConfig(t, runTestAddress(t))
	err := Run(context.Background(), cfg, Options{DrainTimeout: -time.Second, Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
		t.Fatal("provider invoked before budget validation")
		return nil, nil
	}}})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative budget=%v", err)
	}
}
