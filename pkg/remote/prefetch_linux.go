//go:build linux

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

// cachePrefetch belongs to one serial layer application. It overlaps at most
// the immediately following layer with the current layer. Admission reserves
// descriptor bytes before starting I/O, and pendingLayer never writes past that
// size. Thus neither layer count nor an optimistic average permits a byte-budget
// overrun. The current layer gets admission first; a future layer never delays
// it or forces it to bypass the cache. No queue is created without cache.dir.
type cachePrefetch struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cache    *Cache
	layers   []flatten.LayerInput
	budget   int64
	required bool
	mu       sync.Mutex
	used     int64
	next     *cacheCandidate
}

type cacheCandidate struct {
	index int
	done  chan struct{}
	p     *pendingLayer
	err   error
}

func newCachePrefetch(ctx context.Context, cache *Cache, layers []flatten.LayerInput, budget int64, required bool) *cachePrefetch {
	ctx, cancel := context.WithCancel(ctx)
	return &cachePrefetch{ctx: ctx, cancel: cancel, cache: cache, layers: layers, budget: budget, required: required}
}

func (q *cachePrefetch) fits(l flatten.LayerInput) bool {
	return l.Size <= q.budget && (q.cache.maxSize <= 0 || l.Size <= q.cache.maxSize)
}

func (q *cachePrefetch) reserve(size int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if size < 0 || size > q.budget-q.used {
		return false
	}
	q.used += size
	return true
}

func (q *cachePrefetch) release(size int64) {
	q.mu.Lock()
	q.used -= size
	q.mu.Unlock()
}

func (q *cachePrefetch) open(ctx context.Context, index int) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l := q.layers[index]
	var body io.ReadCloser
	if next := q.next; next != nil && next.index == index {
		// The worker owns the file and network until it has really exited.
		// Cancellation closes the network through its context, then joins it.
		<-next.done
		q.next = nil
		if next.err != nil {
			err := next.err
			if next.p != nil {
				err = errors.Join(err, next.p.CompleteLayer(false))
			}
			q.release(l.Size)
			return nil, err
		}
		body = &cacheLease{ReadCloser: &stagedLayer{next.p}, release: func() { q.release(l.Size) }}
	} else {
		h, err := v1.NewHash(l.Digest)
		if err != nil {
			return nil, err
		}
		body, err = q.cache.Get(h)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if errors.Is(err, os.ErrNotExist) {
			if !q.fits(l) {
				if q.required {
					return nil, fmt.Errorf("remote: required cache cannot fit layer %s (%d bytes)", l.Digest, l.Size)
				}
				body, err = l.Open(ctx)
			} else if !q.reserve(l.Size) {
				return nil, fmt.Errorf("remote: current layer cache reservation unavailable")
			} else {
				body, err = q.cache.pendingLayer(ctx, h, l)
				if err != nil {
					q.release(l.Size)
				} else {
					body = &cacheLease{ReadCloser: body, release: func() { q.release(l.Size) }}
				}
			}
			if err != nil {
				return body, err // flatten owns even a body returned with an error
			}
		}
	}
	q.startNext(index)
	return body, nil
}

func (q *cachePrefetch) startNext(current int) {
	i := current + 1
	if q.next != nil || i >= len(q.layers) || q.ctx.Err() != nil {
		return
	}
	l := q.layers[i]
	h, err := v1.NewHash(l.Digest)
	if err != nil || l.Digest == q.layers[current].Digest || !q.fits(l) || q.cache.Has(h) || !q.reserve(l.Size) {
		return
	}
	next := &cacheCandidate{index: i, done: make(chan struct{})}
	q.next = next
	go func() {
		defer close(next.done)
		r, err := q.cache.pendingLayer(q.ctx, h, l)
		if err != nil {
			next.err = err
			return
		}
		next.p = r.(*pendingLayer)
		cancelDone := make(chan struct{})
		stop := context.AfterFunc(q.ctx, func() { next.p.Close(); close(cancelDone) })
		_, err = io.Copy(io.Discard, next.p)
		if !stop() {
			<-cancelDone
		}
		err = errors.Join(err, next.p.Close(), q.ctx.Err())
		if err == nil && next.p.remaining != 0 {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = next.p.file.Seek(0, io.SeekStart)
		}
		next.err = err
	}()
}

func (q *cachePrefetch) close() error {
	q.cancel()
	if next := q.next; next != nil {
		<-next.done
		q.next = nil
		defer q.release(q.layers[next.index].Size)
		if next.p != nil {
			return next.p.CompleteLayer(false)
		}
	}
	return nil
}

// Prefetch only stages compressed bytes. The normal applier still verifies the
// complete compressed stream, decoder, DiffID and close errors before commit.
type stagedLayer struct{ *pendingLayer }

func (s *stagedLayer) Read(p []byte) (int, error) { return s.file.Read(p) }

type cacheLease struct {
	io.ReadCloser
	release func()
	once    sync.Once
	err     error
}

func (r *cacheLease) CompleteLayer(verified bool) error {
	r.once.Do(func() {
		defer r.release()
		r.err = r.ReadCloser.(flatten.LayerCompletion).CompleteLayer(verified)
	})
	return r.err
}
