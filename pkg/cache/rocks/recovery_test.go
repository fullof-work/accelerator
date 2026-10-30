//go:build !no_rocksdb

package rocks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/freq"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	cacheruntime "github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const recoveryChildMode = "ACCELERATOR_ROCKS_RECOVERY_MODE"
const recoveryChildPath = "ACCELERATOR_ROCKS_RECOVERY_PATH"
const recoveryCheckpointPrefix = "RECOVERY_CHECKPOINT "

var recoveryPartitions = []struct {
	name string
	p    store.Partition
	cf   int
}{
	{"chunk", store.PartitionChunk, native.Chunk},
	{"manifest", store.PartitionManifest, native.Manifest},
	{"blob", store.PartitionBlob, native.Blob},
}

func recoveryConfig(path string) cacheruntime.RocksConfig {
	return cacheruntime.RocksConfig{Path: path, DiskBytes: "64MiB", MemRatio: 0.1}
}

func recoveryOpen(t *testing.T, path string) *impl {
	t.Helper()
	s, err := openImpl(recoveryConfig(path), cacheruntime.FreqConfig{
		Counters: "1K", DisableEviction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func recoveryValue(part string, i int) (store.ContentKey, []byte) {
	key := store.ContentKey(sha256.Sum256([]byte(fmt.Sprintf("recovery/%s/%d", part, i))))
	return key, bytes.Repeat(key[:], (256<<10)/len(key))
}

func recoveryFill(t *testing.T, s *impl, first, last int, flush bool) {
	t.Helper()
	for _, part := range recoveryPartitions {
		for i := first; i < last; i++ {
			key, value := recoveryValue(part.name, i)
			if err := s.Fill(context.Background(), part.p, key, value); err != nil {
				t.Fatal(err)
			}
		}
		if flush {
			if err := s.db.Flush(part.cf); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func recoveryCheckpoint(t *testing.T, s *impl) string {
	t.Helper()
	data, err := rawRead(s, cfDefault, []byte(freq.ReservedSketchKey))
	if err != nil || len(data) == 0 {
		t.Fatalf("missing sketch checkpoint: %v", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// A successful Close preserves WAL-free tails; a process crash may discard
// them. The latest unflushed sketch checkpoint must still recover through WAL.
// This tests process termination, not a machine crash or power loss.
func TestCacheRecovery(t *testing.T) {
	for _, mode := range []string{"close", "kill"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "kill" && runtime.GOOS == "windows" {
				t.Skip("requires Unix SIGKILL exit status")
			}
			path := filepath.Join(t.TempDir(), "db")
			t.Setenv(recoveryChildMode, mode)
			t.Setenv(recoveryChildPath, path)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCacheRecoveryProcess$", "-test.v", "-test.count=1", "-test.timeout=50s")
			output, childErr := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("recovery subprocess timed out: %s", output)
			}
			if mode == "kill" {
				var exitErr *exec.ExitError
				if !errors.As(childErr, &exitErr) || exitErr.ExitCode() != -1 {
					t.Fatalf("expected child killed by signal, got %v: %s", childErr, output)
				}
			} else if childErr != nil {
				t.Fatalf("recovery subprocess: %v\n%s", childErr, output)
			}
			var checkpoint string
			for _, line := range strings.Split(string(output), "\n") {
				if strings.HasPrefix(line, recoveryCheckpointPrefix) {
					checkpoint = strings.TrimPrefix(line, recoveryCheckpointPrefix)
				}
			}
			if len(checkpoint) != sha256.Size*2 {
				t.Fatalf("missing completed-write marker: %s", output)
			}
			s := recoveryOpen(t, path)
			defer s.Close()
			if got := recoveryCheckpoint(t, s); got != checkpoint {
				t.Fatalf("latest sketch checkpoint was lost: got %s, want %s", got, checkpoint)
			}
			for _, part := range recoveryPartitions {
				for i := 0; i < 8; i++ {
					key, want := recoveryValue(part.name, i)
					result, blob, err := s.Get(context.Background(), part.p, key)
					if err != nil {
						if blob != nil {
							blob.Release()
						}
						t.Fatal(err)
					}
					wantResult := cache.CacheHit
					if mode == "kill" && i >= 4 {
						wantResult = cache.CacheMiss
					}
					valid := result == wantResult && (result != cache.CacheHit || (blob != nil && bytes.Equal(blob.Bytes(), want)))
					if blob != nil {
						blob.Release()
					}
					if !valid {
						t.Fatalf("%s/%d: result %v, want %v with unchanged bytes", part.name, i, result, wantResult)
					}
				}
			}
		})
	}
}

// The parent starts only this test in a fresh process. No deferred Close runs
// on the kill path, so it cannot accidentally flush the data under test.
func TestCacheRecoveryProcess(t *testing.T) {
	mode := os.Getenv(recoveryChildMode)
	if mode == "" {
		t.Skip("subprocess helper")
	}
	path := os.Getenv(recoveryChildPath)
	if path == "" || (mode != "close" && mode != "kill") {
		t.Fatal("invalid recovery subprocess arguments")
	}
	s := recoveryOpen(t, path)
	recoveryFill(t, s, 0, 4, true)
	s.PersistSketch()
	if err := s.db.Flush(native.Default); err != nil {
		t.Fatal(err)
	}
	oldCheckpoint := recoveryCheckpoint(t, s)
	recoveryFill(t, s, 4, 8, false)
	s.PersistSketch() // Deliberately no default-CF Flush after this checkpoint.
	checkpoint := recoveryCheckpoint(t, s)
	if checkpoint == oldCheckpoint {
		t.Fatal("latest checkpoint must differ from the flushed prefix checkpoint")
	}
	if mode == "close" {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("%s%s\n", recoveryCheckpointPrefix, checkpoint)
	if mode == "kill" {
		self, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		if err := self.Kill(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("SIGKILL unexpectedly returned")
	}
}

// Replacing only this test's DB path with a regular file makes SST creation
// fail with ENOTDIR, without relying on permissions, disk exhaustion or hooks.
func TestCloseReportsFlushFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires renaming an open DB directory")
	}
	root := t.TempDir()
	path, moved := filepath.Join(root, "db"), filepath.Join(root, "moved")
	s := recoveryOpen(t, path)
	defer s.Close()
	recoveryFill(t, s, 0, 4, true)
	recoveryFill(t, s, 4, 8, false)
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() error {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return os.Rename(moved, path)
	}
	defer func() {
		if !restored {
			if err := restore(); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := os.WriteFile(path, []byte("force flush path failure"), 0600); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 4)
	for i := 0; i < cap(errs); i++ {
		go func() { errs <- s.Close() }()
	}
	first := <-errs
	if first == nil || !strings.Contains(first.Error(), "flush chunk:") {
		t.Errorf("Close must report the data flush failure, got %v", first)
	}
	for i := 1; i < cap(errs); i++ {
		if err := <-errs; err != first {
			t.Errorf("concurrent Close returned %v, want the same error %v", err, first)
		}
	}
	if err := s.Close(); err != first {
		t.Errorf("repeated Close returned %v, want %v", err, first)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	restored = true
	// Reopening proves Close released the DB lock despite its flush error.
	reopened := recoveryOpen(t, path)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
