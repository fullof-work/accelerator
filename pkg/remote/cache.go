package remote

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

// Cache is a content-addressed blob cache laid out as a standard OCI image
// layout (oci-layout + index.json + blobs/<algo>/<hex>), so the same dir is
// inspectable with crane/skopeo and shareable across flatten-ctl processes.
//
// The hot path never consults the mutable index.json: a cache hit is simply
// the existence of blobs/<algo>/<hex>. Blobs are written atomically
// (temp + digest-verify + exclusive link), which makes concurrent writers and
// crash recovery safe — ggcr's own layout.WriteBlob writes the final path
// directly (not atomic) and is deliberately not used here. index.json is
// maintained best-effort for tooling; cache correctness never depends on it.
// Size-cap LRU eviction lives in gc.go and keys off blob mtime, refreshed by
// Get so a recently-read blob survives the sweep.
type Cache struct {
	dir     string
	maxSize int64 // bytes; 0 = unlimited
}

// OpenCache opens (or initialises) an OCI-layout cache rooted at dir. The
// layout package is used only to lay down a valid oci-layout + index.json so
// the dir is inspectable with crane/skopeo; blobs are then read/written
// directly (content-addressed, atomic) rather than through layout.Path, whose
// blob writes are non-atomic and whose index.json mutation is not
// concurrency-safe.
func OpenCache(dir string, maxSize int64) (*Cache, error) {
	if dir == "" {
		return nil, fmt.Errorf("remote: cache dir is empty")
	}
	if _, err := layout.FromPath(dir); err != nil {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("remote: mkdir cache %q: %w", dir, err)
		}
		if _, err := layout.Write(dir, empty.Index); err != nil {
			return nil, fmt.Errorf("remote: init cache layout %q: %w", dir, err)
		}
	}
	return &Cache{dir: dir, maxSize: maxSize}, nil
}

func (c *Cache) blobPath(h v1.Hash) string {
	return filepath.Join(c.dir, "blobs", h.Algorithm, h.Hex)
}

// Has reports whether the blob is cached — the hot-path cache-hit test.
// Content-addressed and index-free, hence inherently multi-process safe.
func (c *Cache) Has(h v1.Hash) bool {
	st, err := os.Lstat(c.blobPath(h))
	return err == nil && st.Mode().IsRegular()
}

// Get opens a cached blob and refreshes its mtime so the LRU sweep treats it
// as recently used.
func (c *Cache) Get(h v1.Hash) (io.ReadCloser, error) {
	p := c.blobPath(h)
	st, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("remote: cache object is not a regular file: %s", h)
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	if err = pinBlob(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	a, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	b, err := os.Lstat(p)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if !b.Mode().IsRegular() || !os.SameFile(a, b) || !os.SameFile(a, st) {
		return nil, errors.Join(os.ErrNotExist, f.Close())
	}
	touch(p)
	return f, nil
}

// Put writes rc into the cache atomically, verifying the content hashes to h.
// Two writers of the same content use distinct pending files and keep the
// existing verified inode, so active consumers never retain an unlinked blob.
func (c *Cache) Put(h v1.Hash, rc io.ReadCloser) (_ int64, retErr error) {
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, rc.Close())
		}
	}()
	dir := filepath.Join(c.dir, "blobs", h.Algorithm)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+h.Hex+"-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := pinPendingBlob(tmp); err != nil {
		return 0, errors.Join(err, tmp.Close())
	}

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hasher), rc)
	if err != nil {
		tmp.Close()
		return 0, fmt.Errorf("remote: cache write %s: %w", h, err)
	}
	err = rc.Close()
	closed = true
	if err != nil {
		return 0, errors.Join(err, tmp.Close())
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, err
	}
	unlock, err := lockPublication(filepath.Join(c.dir, ".gc.lock"))
	if err != nil {
		return 0, errors.Join(err, tmp.Close())
	}
	defer unlock()
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if h.Algorithm == "sha256" {
		if got := hex.EncodeToString(hasher.Sum(nil)); got != h.Hex {
			return 0, fmt.Errorf("remote: cache digest mismatch for %s: got sha256:%s", h, got)
		}
	}
	if err := commitBlob(tmpName, c.blobPath(h), n); err != nil {
		return 0, fmt.Errorf("remote: cache commit %s: %w", h, err)
	}
	return n, nil
}

// A duplicate must refer to an existing regular object with the same complete
// bytes. Do not treat EEXIST for a directory or damaged blob as a successful
// commit, and never replace an inode held by another consumer.
func commitBlob(pending, final string, size int64) error {
	if size < 0 || size == math.MaxInt64 {
		return fmt.Errorf("remote: invalid cache object size")
	}
	err := os.Link(pending, final)
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	st, statErr := os.Lstat(final)
	if statErr != nil || !st.Mode().IsRegular() || st.Size() != size {
		return err
	}
	var sums [2][32]byte
	for i, name := range []string{pending, final} {
		f, e := os.Open(name)
		if e != nil {
			return errors.Join(err, e)
		}
		h := sha256.New()
		n, e := io.Copy(h, io.LimitReader(f, size+1))
		e = errors.Join(e, f.Close())
		if e != nil || n != size {
			return errors.Join(err, e)
		}
		copy(sums[i][:], h.Sum(nil))
	}
	if sums[0] != sums[1] {
		return fmt.Errorf("remote: conflicting cache object: %w", err)
	}
	return nil
}

// touch refreshes a blob's access/modify time to now (best-effort), feeding
// the LRU eviction in gc.go.
func touch(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
}
