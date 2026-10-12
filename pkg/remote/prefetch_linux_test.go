//go:build linux

package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

func cacheTestLayer(t *testing.T, data []byte, opens *atomic.Int32) flatten.LayerInput {
	t.Helper()
	return flatten.LayerInput{Digest: hashOf(t, data).String(), Size: int64(len(data)),
		Open: func(context.Context) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(data)), nil
		}}
}

func TestPrefetchByteBudgetAndCurrentPriority(t *testing.T) {
	for _, tc := range []struct {
		name      string
		budget    int64
		duplicate bool
		prefetch  bool
	}{
		{"overlap", 1024, false, true},
		{"current-first", 600, false, false},
		{"oversize-bypass", 100, false, false},
		{"duplicate", 1024, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := OpenCache(t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			var opens [2]atomic.Int32
			data := [][]byte{bytes.Repeat([]byte("a"), 512), bytes.Repeat([]byte("b"), 512)}
			if tc.duplicate {
				data[1] = data[0]
			}
			layers := []flatten.LayerInput{cacheTestLayer(t, data[0], &opens[0]), cacheTestLayer(t, data[1], &opens[1])}
			q := newCachePrefetch(context.Background(), cache, layers, tc.budget, false)
			defer q.close()
			for i := range layers {
				r, err := q.open(context.Background(), i)
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					if (q.next != nil) != tc.prefetch {
						t.Fatalf("prefetch: %v", q.next != nil)
					}
					if q.next != nil {
						<-q.next.done
						if q.next.err != nil {
							t.Fatal(q.next.err)
						}
						if cache.Has(hashOf(t, data[1])) {
							t.Fatal("prefetch EOF published an unverified blob")
						}
						if opens[1].Load() != 1 {
							t.Fatal("future download did not overlap application")
						}
					}
					q.mu.Lock()
					used := q.used
					q.mu.Unlock()
					if used > tc.budget {
						t.Fatalf("reserved %d > %d", used, tc.budget)
					}
				}
				got, err := io.ReadAll(r)
				if err != nil || !bytes.Equal(got, data[i]) {
					t.Fatalf("read: %v", err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if completion, ok := r.(flatten.LayerCompletion); ok {
					if err := completion.CompleteLayer(true); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := q.close(); err != nil {
				t.Fatal(err)
			}
			if q.used != 0 {
				t.Fatalf("reserved bytes left: %d", q.used)
			}
			if tc.duplicate && opens[1].Load() != 0 {
				t.Fatal("duplicate layer downloaded twice")
			}
			pending, _ := filepath.Glob(filepath.Join(cache.dir, "blobs", "sha256", ".tmp-*"))
			if len(pending) != 0 {
				t.Fatal(pending)
			}
		})
	}
}

type cacheBlockingBody struct {
	closed chan struct{}
	once   atomic.Bool
}

func (r *cacheBlockingBody) Read([]byte) (int, error) { <-r.closed; return 0, context.Canceled }
func (r *cacheBlockingBody) Close() error {
	if r.once.CompareAndSwap(false, true) {
		close(r.closed)
	}
	return nil
}

func TestPrefetchCancelJoinsAndRemovesOnlyOwnedPending(t *testing.T) {
	cache, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int32
	layers := []flatten.LayerInput{cacheTestLayer(t, []byte("first"), &opens), cacheTestLayer(t, []byte("second"), &opens)}
	started := make(chan struct{})
	body := &cacheBlockingBody{closed: make(chan struct{})}
	layers[1].Open = func(context.Context) (io.ReadCloser, error) { close(started); return body, nil }
	q := newCachePrefetch(context.Background(), cache, layers, 100, false)
	r, err := q.open(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := r.(flatten.LayerCompletion).CompleteLayer(true); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(cache.dir, "blobs", "sha256", ".tmp-another-writer")
	if err := os.WriteFile(unrelated, []byte("owned elsewhere"), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- q.close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not join cancelled prefetch")
	}
	if !body.once.Load() || q.used != 0 {
		t.Fatal("network or reservation still owned")
	}
	if !cache.Has(hashOf(t, []byte("first"))) || cache.Has(hashOf(t, []byte("second"))) {
		t.Fatal("invalid completion state")
	}
	pending, _ := filepath.Glob(filepath.Join(cache.dir, "blobs", "sha256", ".tmp-*"))
	if len(pending) != 1 || pending[0] != unrelated {
		t.Fatal(pending)
	}
}

func TestRequiredCacheRejectsOversizeBeforeOpen(t *testing.T) {
	cache, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int32
	layer := cacheTestLayer(t, []byte("oversize"), &opens)
	q := newCachePrefetch(context.Background(), cache, []flatten.LayerInput{layer}, 1, true)
	defer q.close()
	if r, err := q.open(context.Background(), 0); err == nil || r != nil {
		t.Fatal("required cache silently bypassed")
	}
	if opens.Load() != 0 {
		t.Fatal("opened rejected layer")
	}
}

func TestPrefetchFailureRemainsUncommitted(t *testing.T) {
	cache, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int32
	layers := []flatten.LayerInput{cacheTestLayer(t, []byte("first"), &opens), cacheTestLayer(t, []byte("second"), &opens)}
	failure := errors.New("prefetch close failure")
	layers[1].Open = func(context.Context) (io.ReadCloser, error) {
		return &metadataBody{Reader: bytes.NewReader([]byte("second")), closeErr: failure}, nil
	}
	q := newCachePrefetch(context.Background(), cache, layers, 100, false)
	defer q.close()
	r, err := q.open(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r)
	r.Close()
	if err := r.(flatten.LayerCompletion).CompleteLayer(true); err != nil {
		t.Fatal(err)
	}
	if r, err := q.open(context.Background(), 1); r != nil || !errors.Is(err, failure) {
		t.Fatalf("error lost: %v", err)
	}
	if cache.Has(hashOf(t, []byte("second"))) || q.used != 0 {
		t.Fatal("failed prefetch retained")
	}
}
