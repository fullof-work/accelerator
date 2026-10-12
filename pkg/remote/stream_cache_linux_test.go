//go:build linux

package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

func TestPendingLayerCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		verified  bool
		closeErr  error
		truncated bool
	}{
		{"success", true, nil, false}, {"unverified", false, nil, false},
		{"close-failure", true, errors.New("close body"), false}, {"truncated", true, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := OpenCache(t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("verified compressed layer")
			h := hashOf(t, data)
			body := &metadataBody{Reader: bytes.NewReader(data), closeErr: tc.closeErr}
			l := flatten.LayerInput{Size: int64(len(data)), Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
			if tc.truncated {
				l.Size++
			}
			r, err := cache.pendingLayer(context.Background(), h, l)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				t.Fatal(err)
			}
			if cache.Has(h) {
				t.Fatal("EOF committed unverified object")
			}
			r.Close()
			if cache.Has(h) {
				t.Fatal("Close committed unverified object")
			}
			err = r.(flatten.LayerCompletion).CompleteLayer(tc.verified)
			if (err != nil) != (tc.closeErr != nil || tc.truncated) {
				t.Fatalf("completion: %v", err)
			}
			if cache.Has(h) != (tc.verified && err == nil) {
				t.Fatal("invalid cache completion state")
			}
			if body.closed != 1 {
				t.Fatalf("body closed %d times", body.closed)
			}
			pending, _ := filepath.Glob(filepath.Join(cache.dir, "blobs", "sha256", ".tmp-*"))
			if len(pending) != 0 {
				t.Fatalf("pending files left: %v", pending)
			}
		})
	}
}

func TestCacheReaderAndPendingLeases(t *testing.T) {
	cache, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("shared cache reader")
	h := hashOf(t, data)
	if _, err := cache.Put(h, io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	r, err := cache.Get(h)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(cache.blobPath(h))
	if _, err := cache.Put(h, io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(cache.blobPath(h))
	if !os.SameFile(before, after) {
		t.Fatal("duplicate replaced a reader's inode")
	}
	old := time.Now().Add(-time.Hour)
	mustChtime(t, cache.blobPath(h), old)
	if _, err := cache.Evict(0); err != nil {
		t.Fatal(err)
	}
	if !cache.Has(h) {
		t.Fatal("GC unlinked an active reader")
	}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reader: %q %v", got, err)
	}
	r.Close()
	if _, err := cache.Evict(0); err != nil {
		t.Fatal(err)
	}
	if cache.Has(h) {
		t.Fatal("idle old blob survived GC")
	}
	f, err := os.CreateTemp(filepath.Join(cache.dir, "blobs", "sha256"), ".tmp-test-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := pinPendingBlob(f); err != nil {
		t.Fatal(err)
	}
	mustChtime(t, f.Name(), old)
	if _, err := cache.Evict(0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.Name()); err != nil {
		t.Fatal("GC removed in-flight writer:", err)
	}
	f.Close()
	if _, err := cache.Evict(0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("abandoned cache write survived GC:", err)
	}
}

func TestPendingCompletionWithConcurrentGC(t *testing.T) {
	for _, verified := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "publish"}[verified], func(t *testing.T) {
			cache, err := OpenCache(t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("old pending layer remains pinned until publication")
			h := hashOf(t, data)
			r, err := cache.pendingLayer(context.Background(), h, flatten.LayerInput{
				Size: int64(len(data)), Open: func(context.Context) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(data)), nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			p := r.(*pendingLayer)
			if _, err := io.Copy(io.Discard, p); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-time.Hour)
			mustChtime(t, p.name, old)
			unlock, err := lockFile(p.gcLock)
			if err != nil {
				t.Fatal(err)
			}
			locked := true
			defer func() {
				if locked {
					unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- p.CompleteLayer(verified) }()
			if !verified {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("aborted layer waits for unrelated GC")
				}
			} else {
				select {
				case err := <-done:
					t.Fatalf("published while GC held its exclusive lock: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				if removed, err := removeIdleBlob(p.name, time.Now()); removed || err != nil {
					t.Fatalf("GC reclaimed the pending writer before publication: %v, %v", removed, err)
				}
				unlock()
				locked = false
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			if cache.Has(h) != verified {
				t.Fatal("incorrect completion state")
			}
			if _, err := os.Stat(p.name); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("pending file survived completion:", err)
			}
		})
	}
}
