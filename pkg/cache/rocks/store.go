//go:build !no_rocksdb

package rocks

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/freq"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ErrReadOnly is returned by writes to a store opened with OpenReadOnly.
var ErrReadOnly = errors.New("rocks: store is read-only")

// errClosed distinguishes use after shutdown from an ordinary cache miss.
var errClosed = errors.New("rocks: store is closed")

// impl owns a private native DB and the existing Go frequency sketch.
// mu gates the full request, including pin-to-BlobPool copying and Touch.
// The sketch persister is joined separately before native shutdown.
type impl struct {
	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
	db        *native.DB
	cfh       map[string]int
	sketch    *freq.Sketch
	readOnly  bool
}

var _ Interface = (*impl)(nil)

// Open opens the fixed cache CFs and restores their frequency sketch before
// enabling eviction. Unsupported legacy sketch encodings still start empty.
func Open(cfg runtime.RocksConfig, freqCfg runtime.FreqConfig) (Interface, error) {
	return openImpl(cfg, freqCfg)
}

func openImpl(cfg runtime.RocksConfig, freqCfg runtime.FreqConfig) (*impl, error) {
	filter := newSketchCompactionFilter()
	// The callback remains unarmed through DB recovery and sketch restoration.
	db, err := native.Open(cfg.Path, buildConfig(cfg), false, filter.Evict)
	if err != nil {
		return nil, fmt.Errorf("rocks: open %s: %w", cfg.Path, err)
	}
	var data []byte
	_, getErr := db.Read(native.Default, []byte(freq.ReservedSketchKey), func(n int) []byte {
		data = make([]byte, n)
		return data
	})
	if getErr != nil {
		// A storage error is not missing/legacy metadata. Leave the filter
		// unarmed and tear down all resources rather than evicting blindly.
		_ = db.Close()
		return nil, fmt.Errorf("rocks: restore sketch: %w", getErr)
	}
	inst := &impl{db: db, cfh: cfIndexes()}
	sketch := buildSketch(freqCfg, diskBytes(cfg), func(data []byte) {
		// No lifecycle lock: Close joins this goroutine before closing db.
		if err := db.Put(native.Default, []byte(freq.ReservedSketchKey), data); err != nil {
			log.Printf("rocks: background sketch persist failed: %v", err)
		}
	})
	inst.sketch = sketch
	filter.SetSketch(sketch)

	if len(data) > 0 {
		if err := sketch.Unmarshal(data); err != nil {
			log.Printf("rocks: discarding persisted sketch: %v", err)
		}
	}
	if !freqCfg.DisableEviction {
		filter.Arm()
	}
	return inst, nil
}

// OpenReadOnly uses RocksDB OpenForReadOnly with error_if_wal_file_exists=false,
// matching the existing API. It may replay WAL into memory; it is not a
// secondary instance. No sketch is constructed and writes are rejected.
func OpenReadOnly(cfg runtime.RocksConfig) (Interface, error) {
	return openReadOnlyImpl(cfg)
}

func openReadOnlyImpl(cfg runtime.RocksConfig) (*impl, error) {
	db, err := native.Open(cfg.Path, buildConfig(cfg), true, nil)
	if err != nil {
		return nil, fmt.Errorf("rocks: open read-only %s: %w", cfg.Path, err)
	}
	return &impl{db: db, cfh: cfIndexes(), readOnly: true}, nil
}

func cfIndexes() map[string]int {
	return map[string]int{cfDefault: native.Default, cfChunk: native.Chunk,
		cfManifest: native.Manifest, cfBlob: native.Blob}
}

func (s *impl) Sketch() *freq.Sketch { return s.sketch }

// get is synchronous; ctx cannot cancel an in-progress native Get.
func (s *impl) get(ctx context.Context, ns string, key []byte) (cache.Blob, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, errClosed
	}
	if s.sketch != nil {
		s.sketch.Touch(key)
	}
	cf, ok := s.cfh[ns]
	if !ok {
		cf = native.Chunk
	}
	var blob cache.Blob
	found, err := s.db.Read(cf, key, func(n int) []byte {
		// Preserve the former store's empty-value-as-miss behavior.
		if n == 0 {
			return nil
		}
		var buf []byte
		buf, blob = poolForNS(ns).Alloc(n)
		return buf
	})
	if err != nil {
		return nil, false, fmt.Errorf("rocks: get: %w", err)
	}
	return blob, found && blob != nil, nil
}

func (s *impl) put(ctx context.Context, ns string, key, value []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}
	if s.readOnly {
		return ErrReadOnly
	}
	cf, ok := s.cfh[ns]
	if !ok {
		cf = native.Chunk
	}
	if err := s.db.Put(cf, key, value); err != nil {
		return fmt.Errorf("rocks: put: %w", err)
	}
	if s.sketch != nil {
		// Successful object/shard payload drives soft frequency aging using
		// the same disk budget as cache sizing. Overwrites count as writes;
		// default-CF sketch persistence bypasses this data path.
		s.sketch.TouchBytes(key, uint64(len(value)))
	}
	return nil
}

func (s *impl) PersistSketch() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.closed {
		_ = s.persistSketch()
	}
}

func (s *impl) persistSketch() error {
	if s.sketch == nil {
		return nil
	}
	return s.db.Put(native.Default, []byte(freq.ReservedSketchKey), s.sketch.Marshal())
}

// Close first drains requests, then joins sketch workers, persists their final
// state and destroys the native owner. DB destruction joins compactions before
// the bridge releases the filter and its cgo.Handle. Concurrent Close is safe.
func (s *impl) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		if s.sketch != nil {
			_ = s.sketch.Close()
		}
		s.closeErr = errors.Join(s.persistSketch(), s.db.Close())
	})
	return s.closeErr
}

// compactAll is used by tests; it shares the request lifecycle gate.
func (s *impl) compactAll() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errClosed
	}
	for _, cf := range s.cfh {
		if err := s.db.Compact(cf); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// Interface methods: cache.Tier (object-level) and cache.ShardTier
// (shard-level with fixed-suffix key encoding).
// ---------------------------------------------------------------------

// Get implements cache.Getter. Object-level read routed to the CF
// matching store.Partition.
func (s *impl) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	blob, found, err := s.get(ctx, partitionToCF(p), key[:])
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	if !found {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, blob, nil
}

// Fill implements cache.Filler. Object-level write. Synchronous —
// `data` is safe to release after Fill returns.
func (s *impl) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	return s.put(ctx, partitionToCF(p), key[:], data)
}

// GetShard implements cache.ShardGetter. Shard-level read keyed by
// content hash only; the returned blob's first ShardPrefixSize bytes
// carry the shard (idx, total) metadata as stored by FillShard.
//
// The on-disk rocksdb key is `content_key[:32] || shardKeySuffix` —
// a fixed-suffix byte differentiates shard entries from object
// entries that share the same CF, so object and shard ops never
// collide even when the same cache-ctl daemon serves both (e.g. the
// unified-handler mode used by e2e tests).
func (s *impl) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	blob, found, err := s.get(ctx, partitionToCF(p), shardKey(key))
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	if !found {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, blob, nil
}

// FillShard implements cache.ShardFiller. The value bytes are written
// verbatim; the caller is responsible for prefixing value with the
// [idx][total] header (see cache.EncodeShardPrefix). A peer stores
// exactly one shard per content_key — repeated FillShard for the same
// key overwrites, consistent with the invariant that under normal
// operation a peer holds at most one (idx, total, data) per content.
func (s *impl) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	return s.put(ctx, partitionToCF(p), shardKey(key), value)
}

// shardKey produces the rocksdb key for a shard: the 32-byte content
// key plus a fixed marker byte. The marker discriminates shards from
// 32-byte object keys in the same CF; its value (shardKeySuffix) is
// implementation detail — any fixed byte works.
const shardKeySuffix byte = 0x00

func shardKey(key store.ContentKey) []byte {
	k := make([]byte, 33)
	copy(k[:32], key[:])
	k[32] = shardKeySuffix
	return k
}

// buildSketch constructs a freq.Sketch from runtime.FreqConfig, applying
// defaults for unset fields. A zero FreqConfig yields a Sketch with all
// frequency defaults; resetBytes is the existing Rocks disk budget, not
// another user-selected frequency parameter.
//
// persistFn is forwarded directly to freq.Config.PersistFn. freq.New's
// existing gate (PersistInterval > 0 && PersistFn != nil) decides
// whether to start the background persistLoop goroutine — so passing
// nil here (e.g. when persistence is disabled) is safe and keeps
// behaviour identical to the pre-refactor "Close-only persist" path.
func buildSketch(cfg runtime.FreqConfig, resetBytes uint64, persistFn func([]byte)) *freq.Sketch {
	counters := parseCountWithDefault(cfg.Counters, freq.DefaultCounters)
	resetAfter := parseCountWithDefault(cfg.ResetAfter, freq.DefaultResetAfter)
	resetInterval, _ := time.ParseDuration(cfg.ResetInterval)
	persistInterval, _ := time.ParseDuration(cfg.PersistInterval)
	threshold := uint8(cfg.EvictThreshold)
	if threshold == 0 {
		threshold = freq.DefaultEvictThreshold
	}
	return freq.New(freq.Config{
		Counters:        counters,
		ResetAfter:      resetAfter,
		ResetBytes:      resetBytes,
		ResetInterval:   resetInterval,
		EvictThreshold:  threshold,
		PersistInterval: persistInterval,
		PersistFn:       persistFn,
	})
}

func parseCountWithDefault(s string, def uint64) uint64 {
	if s == "" {
		return def
	}
	v, err := util.ParseSize(s)
	if err != nil || v == 0 {
		return def
	}
	return v
}
