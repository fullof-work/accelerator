package remote

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// evictLowWatermark is the fraction of maxSize the cache is pruned down
	// to once it exceeds maxSize, so eviction runs in batches rather than on
	// every byte over the cap.
	evictLowWatermark = 0.85
	// evictGracePeriod protects just-written blobs from being evicted out
	// from under a concurrent pull that is about to apply them.
	evictGracePeriod = 5 * time.Minute
)

type blobInfo struct {
	path  string
	size  int64
	mtime time.Time
}

// scanBlobs walks blobs/<algo>/ collecting regular blob files (skipping the
// .tmp-* in-flight writes) and their total size.
func (c *Cache) scanBlobs() ([]blobInfo, int64, error) {
	root := filepath.Join(c.dir, "blobs")
	var (
		infos []blobInfo
		total int64
	)
	err := filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if fi.IsDir() {
			return nil
		}
		if !fi.Mode().IsRegular() {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), ".tmp-") {
			return nil // in-flight write, not a committed blob
		}
		infos = append(infos, blobInfo{path: path, size: fi.Size(), mtime: fi.ModTime()})
		total += fi.Size()
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

// CacheStats summarises the cache for `cache info`.
type CacheStats struct {
	Dir       string
	Blobs     int
	TotalSize int64
	MaxSize   int64 // 0 = unlimited
}

// Stats reports the cache's blob count and total size.
func (c *Cache) Stats() (CacheStats, error) {
	infos, total, err := c.scanBlobs()
	if err != nil {
		return CacheStats{}, err
	}
	return CacheStats{Dir: c.dir, Blobs: len(infos), TotalSize: total, MaxSize: c.maxSize}, nil
}

// MaybeEvict prunes the cache to evictLowWatermark*maxSize when its total
// size exceeds maxSize. No-op when maxSize is 0 (unlimited). Safe under
// concurrent flatten-ctl processes: the prune runs under a directory flock
// and skips blobs younger than the grace period.
func (c *Cache) MaybeEvict() error {
	if c.maxSize <= 0 {
		return nil
	}
	_, total, err := c.scanBlobs()
	if err != nil {
		return err
	}
	if total <= c.maxSize {
		return nil
	}
	target := int64(float64(c.maxSize) * evictLowWatermark)
	return c.evictTo(target)
}

// Evict prunes the cache to at most target bytes (target<=0 evicts every
// eligible blob), returning the number of bytes freed. Used by `cache gc`.
func (c *Cache) Evict(target int64) (int64, error) {
	_, before, err := c.scanBlobs()
	if err != nil {
		return 0, err
	}
	if err := c.evictTo(target); err != nil {
		return 0, err
	}
	_, after, err := c.scanBlobs()
	if err != nil {
		return 0, err
	}
	return before - after, nil
}

// evictTo removes least-recently-used blobs (by mtime) until the total is at
// or below target, under the GC flock and skipping blobs within the grace
// period. Re-scans under the lock so it never acts on a stale view.
func (c *Cache) evictTo(target int64) error {
	unlock, err := lockFile(filepath.Join(c.dir, ".gc.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	if err := filepath.Walk(filepath.Join(c.dir, "blobs"), func(path string, fi os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if fi.Mode().IsRegular() && strings.HasPrefix(fi.Name(), ".tmp-") {
			_, err = removeIdleBlob(path, time.Now().Add(-evictGracePeriod))
		}
		return err
	}); err != nil {
		return err
	}

	infos, total, err := c.scanBlobs()
	if err != nil {
		return err
	}
	if total <= target {
		return nil
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].mtime.Before(infos[j].mtime) })
	cutoff := time.Now().Add(-evictGracePeriod)
	for _, b := range infos {
		if total <= target {
			break
		}
		if b.mtime.After(cutoff) {
			continue // within grace period — a concurrent pull may need it
		}
		removed, err := removeIdleBlob(b.path, cutoff)
		if err != nil {
			return err
		}
		if removed {
			total -= b.size
		}
	}
	return nil
}
