//go:build linux

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

// Prepare uses the pinned streaming input and an explicit persistent cache, if
// configured. With no cache.dir it stores no compressed layers. Cache candidates
// become hits only after the complete layer, decoder and body are verified and
// closed by flatten. The returned PreparedImage owns the private rootfs and plan;
// it never owns or removes persistent cached blobs.
func (c *Config) Prepare(ctx context.Context, res *Resolved, opts flatten.Options) (*flatten.PreparedImage, error) {
	in, err := c.Stream(ctx, res)
	if err != nil {
		return nil, err
	}
	if c.Cache.Dir == "" {
		return flatten.PrepareImage(ctx, in, opts)
	}
	cache, err := OpenCache(c.Cache.Dir, c.maxSize)
	if err != nil {
		return nil, err
	}
	budget := c.cacheInFlight
	if budget == 0 {
		budget = 256 << 20
	}
	queue := newCachePrefetch(ctx, cache, in.Layers(), budget, c.Cache.Required)
	layers := in.Layers()
	for i := range layers {
		layers[i].Open = func(ctx context.Context) (io.ReadCloser, error) { return queue.open(ctx, i) }
	}
	in, err = flatten.NewImageInput(in.ConfigJSON(), layers)
	if err != nil {
		return nil, errors.Join(err, queue.close())
	}
	p, err := flatten.PrepareImage(ctx, in, opts)
	err = errors.Join(err, queue.close())
	if err != nil {
		if p != nil {
			err = errors.Join(err, p.Close())
		}
		return nil, err
	}
	if err = cache.MaybeEvict(); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

type pendingLayer struct {
	body                    io.ReadCloser
	file                    *os.File
	name, final             string
	gcLock                  string
	remaining               int64
	size                    int64
	closeOnce, completeOnce sync.Once
	closeErr, completeErr   error
}

func (c *Cache) pendingLayer(ctx context.Context, h v1.Hash, layer flatten.LayerInput) (_ io.ReadCloser, retErr error) {
	dir := filepath.Dir(c.blobPath(h))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, ".tmp-"+h.Hex+"-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, f.Close(), os.Remove(f.Name()))
		}
	}()
	if err := pinPendingBlob(f); err != nil {
		return nil, err
	}
	body, err := layer.Open(ctx)
	if err != nil {
		if body != nil {
			err = errors.Join(err, body.Close())
		}
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("remote: nil layer body")
	}
	return &pendingLayer{body: body, file: f, name: f.Name(), final: c.blobPath(h), gcLock: filepath.Join(c.dir, ".gc.lock"), remaining: layer.Size, size: layer.Size}, nil
}

func (p *pendingLayer) Read(b []byte) (int, error) {
	n, err := p.body.Read(b)
	if int64(n) > p.remaining {
		return n, fmt.Errorf("remote: pending cache layer exceeds descriptor size")
	}
	if n > 0 {
		written, e := p.file.Write(b[:n])
		if e == nil && written != n {
			e = io.ErrShortWrite
		}
		if e != nil {
			return n, e
		}
		p.remaining -= int64(n)
	}
	return n, err
}

func (p *pendingLayer) Close() error {
	// Cancellation can call Close while Read is still writing to the cache.
	// Only close the network here; CompleteLayer joins the read/applier first.
	p.closeOnce.Do(func() { p.closeErr = p.body.Close() })
	return p.closeErr
}

func (p *pendingLayer) CompleteLayer(verified bool) error {
	p.completeOnce.Do(func() {
		p.completeErr = p.Close()
		if verified && p.remaining != 0 {
			p.completeErr = errors.Join(p.completeErr, fmt.Errorf("remote: incomplete pending cache layer"))
		}
		if verified && p.completeErr == nil {
			p.completeErr = p.file.Sync() // persistent-cache durability, as in Put
		}
		if !verified || p.completeErr != nil {
			// Abort under the inode lease. No publication follows, so waiting
			// for another process's GC would only delay cancellation/cleanup.
			p.completeErr = errors.Join(p.completeErr, os.Remove(p.name), p.file.Close())
			return
		}
		// Check writer Close before publishing, without letting GC remove an
		// old pending name in the interval after its inode lease is released.
		unlock, lockErr := lockPublication(p.gcLock)
		if lockErr == nil {
			defer unlock()
		}
		p.completeErr = errors.Join(p.completeErr, lockErr)
		p.completeErr = errors.Join(p.completeErr, p.file.Close())
		if verified && p.completeErr == nil {
			// Publish without replacing a concurrent reader's inode. A duplicate
			// digest keeps the already committed object, avoiding deleted open FDs.
			if err := commitBlob(p.name, p.final, p.size); err != nil {
				p.completeErr = err
			}
		}
		p.completeErr = errors.Join(p.completeErr, os.Remove(p.name))
	})
	return p.completeErr
}
