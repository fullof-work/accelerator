package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/kuasar-sandbox/accelerator/internal/layercompression"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

// Resolved is a registry reference pinned to a concrete, platform-selected
// image-manifest digest.
type Resolved struct {
	Ref    name.Reference  // the reference as given (tag or digest)
	Repo   name.Repository // owning repository (where referrers live)
	Digest name.Digest     // repo@sha256:<image-manifest-digest>
	Hash   v1.Hash         // the image-manifest digest
}

// LooksLikeReference reports whether s parses as a registry image reference.
// flatten-ctl uses it (after ruling out stdin and an on-disk file) to decide
// a positional argument is a remote pull rather than a local docker-archive.
func LooksLikeReference(s string) bool {
	_, err := name.ParseReference(s)
	return err == nil
}

func nameOpts(insecure bool) []name.Option {
	if insecure {
		return []name.Option{name.Insecure}
	}
	return nil
}

func (c *Config) pullJobs() int {
	if c.PullJobs < 1 {
		return defaultPullJobs
	}
	return c.PullJobs
}

func (c *Config) remoteOpts(ctx context.Context) []ggcrremote.Option {
	opts := []ggcrremote.Option{
		ggcrremote.WithContext(ctx),
		ggcrremote.WithAuth(authenticator()),
		ggcrremote.WithPlatform(c.platform),
		ggcrremote.WithJobs(c.pullJobs()),
	}
	// A custom transport (TLS CA / skip-verify) must apply to every request,
	// including the CDN blob redirects where MITM proxies swap the cert.
	if c.transport != nil {
		opts = append(opts, ggcrremote.WithTransport(c.transport))
	}
	return opts
}

// Resolve parses ref, contacts the registry, selects the configured platform
// from an index (or takes the single manifest), and returns the pinned
// image-manifest digest. Resolving a moving tag here is what lets callers
// print and persist an immutable repo@sha256 for reproducibility.
func (c *Config) Resolve(ctx context.Context, ref string) (*Resolved, error) {
	r, err := name.ParseReference(ref, nameOpts(c.Insecure)...)
	if err != nil {
		return nil, fmt.Errorf("remote: parse ref %q: %w", ref, err)
	}
	var resolved *Resolved
	err = c.retryPull(ctx, "resolve "+ref, metadataRetryDelay, func() error {
		desc, err := ggcrremote.Get(r, c.metadataOpts(ctx)...)
		if err != nil {
			return err
		}
		img, err := desc.Image()
		if err != nil {
			return fmt.Errorf("select platform %s: %w", c.platform, err)
		}
		d, err := img.Digest()
		if err != nil {
			return err
		}
		resolved = &Resolved{Ref: r, Repo: r.Context(), Digest: r.Context().Digest(d.String()), Hash: d}
		return nil
	})
	return resolved, err
}

// Pull ensures every blob of the resolved image (config + layers) is present
// in cache — downloading missing ones concurrently (bounded by PullJobs),
// each digest-verified and written atomically — then returns a flatten.Source
// that streams the layers (using the shared content-based decoder) from
// cache. Downloads run in parallel; flatten then applies layers sequentially.
func (c *Config) Pull(ctx context.Context, res *Resolved, cache *Cache) (flatten.Source, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var img v1.Image
	var mfst *v1.Manifest
	err := c.retryPull(ctx, "manifest "+res.Digest.String(), metadataRetryDelay, func() error {
		var err error
		img, err = ggcrremote.Image(res.Digest, c.remoteOpts(ctx)...)
		if err != nil {
			return err
		}
		mfst, err = img.Manifest()
		return err
	})
	if err != nil {
		return nil, err
	}

	// RawConfigFile in ggcr v0.20.6 can memoize partial bytes on a failed
	// ReadAll. Reuse the already-fetched image on the first attempt, then use
	// a fresh image after a failure so partial config bytes cannot be reused.
	var cfgBytes []byte
	configAttempt := 0
	err = c.retryPull(ctx, "config "+mfst.Config.Digest.String(), metadataRetryDelay, func() error {
		candidate := img
		if configAttempt > 0 {
			var err error
			candidate, err = ggcrremote.Image(res.Digest, c.remoteOpts(ctx)...)
			if err != nil {
				return err
			}
		}
		configAttempt++
		var err error
		cfgBytes, err = candidate.RawConfigFile()
		return err
	})
	if err != nil {
		return nil, err
	}
	if !cache.Has(mfst.Config.Digest) {
		if _, err := cache.Put(mfst.Config.Digest, io.NopCloser(bytes.NewReader(cfgBytes))); err != nil {
			return nil, err
		}
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("remote: layers %s: %w", res.Digest, err)
	}

	// Pass 1: resolve every layer's digest and note which ones are cache
	// misses, so the download progress total is known up front.
	refs := make([]layerRef, len(layers))
	var misses []int
	for i, l := range layers {
		d, err := l.Digest()
		if err != nil {
			return nil, fmt.Errorf("remote: layer digest: %w", err)
		}
		refs[i] = layerRef{digest: d}
		if !cache.Has(d) {
			misses = append(misses, i)
		}
	}

	// Pass 2: download the misses concurrently (bounded by PullJobs),
	// reporting each completion through OnPullProgress.
	toPull := len(misses)
	sem := make(chan struct{}, c.pullJobs())
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errVal error
		done   int
	)
	record := func(err error) {
		mu.Lock()
		if errVal == nil {
			errVal = err
			cancel()
		}
		mu.Unlock()
	}
downloadLoop:
	for _, i := range misses {
		l := layers[i]
		d := refs[i].digest
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break downloadLoop
		}
		wg.Add(1)
		go func(l v1.Layer, d v1.Hash) {
			defer wg.Done()
			defer func() { <-sem }()
			err := c.retryPull(ctx, "layer "+d.String(), layerRetryDelay, func() error {
				// A failed transfer starts at byte zero on retry. Range resume
				// would reduce the cost for large layers, but needs cache support.
				rc, err := l.Compressed()
				if err != nil {
					return err
				}
				// Put closes the reader and removes partial temp files on error.
				_, err = cache.Put(d, rc)
				return err
			})
			if err != nil {
				record(err)
				return
			}

			mu.Lock()
			done++
			n := done
			mu.Unlock()
			if c.OnPullProgress != nil {
				c.OnPullProgress(n, toPull)
			}
		}(l, d)
	}
	wg.Wait()
	if errVal != nil {
		return nil, errVal
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &registrySource{cache: cache, configJSON: cfgBytes, layers: refs}, nil
}

type layerRef struct {
	digest v1.Hash
}

// registrySource is a flatten.Source backed by cached layer blobs.
type registrySource struct {
	cache      *Cache
	configJSON []byte
	layers     []layerRef
}

func (s *registrySource) ConfigJSON() ([]byte, error) { return s.configJSON, nil }

func (s *registrySource) Layers() ([]flatten.LayerOpener, error) {
	openers := make([]flatten.LayerOpener, 0, len(s.layers))
	for _, lr := range s.layers {
		openers = append(openers, func() (io.ReadCloser, error) {
			f, err := s.cache.Get(lr.digest)
			if err != nil {
				return nil, fmt.Errorf("remote: open cached layer %s: %w", lr.digest, err)
			}
			// The returned stream owns the cached file and decoder.
			return decompress(f, lr.digest)
		})
	}
	return openers, nil
}

// decompress uses the same content detection and decoder as streaming Prepare
// and Docker-save input. It retains Pull's ownership of the cached layer body.
func decompress(r io.ReadCloser, d v1.Hash) (io.ReadCloser, error) {
	decoder, _, err := layercompression.Open(r, layercompression.Options{})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("remote: layer %s: %w", d, err), r.Close())
	}
	return &decodedLayer{ReadCloser: decoder, body: r}, nil
}

type decodedLayer struct {
	io.ReadCloser
	body io.Closer
}

func (r *decodedLayer) Close() error {
	return errors.Join(r.body.Close(), r.ReadCloser.Close())
}
