package freq

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Sketch is a lock-free Count-Min Sketch for TinyLFU-style frequency tracking.
//
// Design highlights:
//   - Touch / Estimate / Reset are all lock-free. Touch uses CAS on packed
//     uint64 words (16 × 4-bit counters per word); Reset publishes a new
//     generation through one atomic pointer. Concurrent resets of the same
//     generation coalesce; none of the three paths waits for another.
//   - Reset preserves rolling history: the old active CMS becomes prev, and
//     Estimate returns active + prev/2. Two consecutive Resets discard the
//     oldest generation.
//   - The CMS eats the full key []byte; there is no upstream key→uint64
//     projection. 4 independent row hashes are derived from FNV-1a with
//     row-specific starting state.
//
// Usage:
//   - Call Touch on every Get/Put to record access.
//   - CompactionFilter calls ShouldEvict to decide whether to remove a key.
//   - Halving is triggered by resetAfter (count), resetBytes (successful
//     write payload, when configured), or the existing resetInterval ticker.
type Sketch struct {
	// A single publication keeps active, prev and their progress together.
	current atomic.Pointer[generation]

	width          uint64
	seeds          [cmsDepth]uint64
	evictThreshold uint8

	resetAfter uint64
	resetBytes uint64

	resetInterval time.Duration
	stopTimer     chan struct{}

	persistInterval time.Duration
	persistFn       func(data []byte)
	stopPersist     chan struct{}

	// bgWg joins background goroutines (timerLoop + persistLoop) on
	// Close. Without this, persistLoop's final Marshal+persistFn call
	// can race with the caller tearing down the resources that
	// persistFn captures (e.g. RocksDB WriteOptions).
	bgWg sync.WaitGroup
}

// Progress belongs to the active CMS, so any kind of rollover starts both
// clocks afresh. An operation already using an old generation can finish
// there while another goroutine publishes its successor.
type generation struct {
	active   *cms
	prev     *cms
	samples  atomic.Uint64
	bytes    atomic.Uint64
	rotating atomic.Bool
}

// Config holds Sketch creation parameters.
type Config struct {
	Counters        uint64        // number of 4-bit counters (default 8M)
	ResetAfter      uint64        // halving after this many Touch calls (default 1M)
	ResetBytes      uint64        // successful write payload per generation (0 = disabled)
	ResetInterval   time.Duration // halving on this interval (0 = disabled)
	EvictThreshold  uint8         // CompactionFilter threshold (default 1)
	PersistInterval time.Duration // sketch persistence interval (0 = disabled)
	PersistFn       func([]byte)  // callback to persist serialised sketch
}

// New creates a Sketch with the given configuration.
func New(cfg Config) *Sketch {
	if cfg.Counters == 0 {
		cfg.Counters = DefaultCounters
	}
	if cfg.ResetAfter == 0 {
		cfg.ResetAfter = DefaultResetAfter
	}
	if cfg.EvictThreshold == 0 {
		cfg.EvictThreshold = DefaultEvictThreshold
	}

	width := nextPow2(cfg.Counters)
	seeds := [cmsDepth]uint64{0xa5a5a5a5, 0x5a5a5a5a, 0x12345678, 0x87654321}

	s := &Sketch{
		width:           width,
		seeds:           seeds,
		evictThreshold:  cfg.EvictThreshold,
		resetAfter:      cfg.ResetAfter,
		resetBytes:      cfg.ResetBytes,
		resetInterval:   cfg.ResetInterval,
		persistInterval: cfg.PersistInterval,
		persistFn:       cfg.PersistFn,
	}
	s.current.Store(&generation{active: newCms(width, seeds)})
	// prev starts nil — Estimate must nil-check.

	if cfg.ResetInterval > 0 {
		s.stopTimer = make(chan struct{})
		s.bgWg.Add(1)
		go s.timerLoop()
	}

	if cfg.PersistInterval > 0 && cfg.PersistFn != nil {
		s.stopPersist = make(chan struct{})
		s.bgWg.Add(1)
		go s.persistLoop()
	}

	return s
}

// Touch records an access for the given key. O(len(key)).
// It does not advance the successful-write byte clock.
func (s *Sketch) Touch(key []byte) {
	s.TouchBytes(key, 0)
}

// TouchBytes records one access and its successfully written payload size.
// Repeated writes count again; this measures write turnover, not resident
// space. A single call can roll at most one generation, even if n exceeds
// ResetBytes. Count and byte limits are checked together to avoid aging the
// same access twice. Neither progress counter is part of the checkpoint.
func (s *Sketch) TouchBytes(key []byte, n uint64) {
	g := s.current.Load()
	g.active.increment(key)
	samples := g.samples.Add(1)
	byteLimitReached := false
	if n > 0 && s.resetBytes > 0 {
		byteLimitReached = g.addBytes(n, s.resetBytes)
	}
	if samples >= s.resetAfter || byteLimitReached {
		s.rotate(g)
	}
}

// addBytes saturates at the limit rather than wrapping or carrying a large
// write into several empty generations. The subtraction is safe because
// every stored value is at most limit.
func (g *generation) addBytes(n, limit uint64) bool {
	for {
		old := g.bytes.Load()
		if old >= limit {
			return true
		}
		next := limit
		if n < limit-old {
			next = old + n
		}
		if g.bytes.CompareAndSwap(old, next) {
			return next == limit
		}
	}
}

// Estimate returns the approximate access frequency of the key, blending
// the active generation with a half-weight view of the previous generation.
func (s *Sketch) Estimate(key []byte) uint8 {
	g := s.current.Load()
	v := g.active.estimate(key)
	if p := g.prev; p != nil {
		v += p.estimate(key) >> 1
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}

// ShouldEvict returns true if the key's frequency is at or below the
// configured eviction threshold.
func (s *Sketch) ShouldEvict(key []byte) bool {
	return s.Estimate(key) <= s.evictThreshold
}

// Reset rolls the active generation into prev and installs a fresh active.
// Concurrent requests to reset the same generation coalesce. A later reset
// still advances the new generation, including an adjacent timer tick; the
// timer's existing schedule is not postponed by count or byte rollovers.
func (s *Sketch) Reset() {
	s.rotate(s.current.Load())
}

func (s *Sketch) rotate(g *generation) {
	if !g.rotating.CompareAndSwap(false, true) {
		return
	}
	// Claim before allocating: losing triggers neither block nor allocate
	// another full CMS. Old-generation operations may keep recording there.
	next := &generation{active: newCms(s.width, s.seeds), prev: g.active}
	s.current.CompareAndSwap(g, next)
}

// Marshal serialises the active generation for persistence. prev is
// intentionally dropped — it is historical rolling state with no value
// after a restart.
//
// Format: [version(1)] [width(8)] [row0 words] [row1 words] [row2 words] [row3 words]
// version is sketchFormatVersion = 0x02. Words are little-endian uint64.
// Per-word atomic.Load — not a linearisable snapshot, which CMS tolerates.
func (s *Sketch) Marshal() []byte {
	c := s.current.Load().active
	wordsPerRow := len(c.rows[0])
	bytesPerRow := wordsPerRow * 8
	buf := make([]byte, 1+8+cmsDepth*bytesPerRow)
	buf[0] = sketchFormatVersion
	binary.LittleEndian.PutUint64(buf[1:9], c.width)
	off := 9
	for i := 0; i < cmsDepth; i++ {
		for j := 0; j < wordsPerRow; j++ {
			w := atomic.LoadUint64(&c.rows[i][j])
			binary.LittleEndian.PutUint64(buf[off:], w)
			off += 8
		}
	}
	return buf
}

// Unmarshal restores sketch state from previously persisted data. Only
// active is restored; prev stays empty. Intended to be called on a freshly
// constructed Sketch before it is exposed to concurrent access.
func (s *Sketch) Unmarshal(data []byte) error {
	if len(data) < 9 {
		return fmt.Errorf("freq: data too short (%d bytes)", len(data))
	}
	if data[0] != sketchFormatVersion {
		return fmt.Errorf("freq: unsupported sketch format version 0x%02x (legacy data discarded)", data[0])
	}
	width := binary.LittleEndian.Uint64(data[1:9])
	if width != s.width {
		return fmt.Errorf("freq: width mismatch: persisted %d, current %d", width, s.width)
	}
	wordsPerRow := int((width + nibblesPerWord - 1) / nibblesPerWord)
	bytesPerRow := wordsPerRow * 8
	expected := 9 + cmsDepth*bytesPerRow
	if len(data) != expected {
		return fmt.Errorf("freq: data size %d, expected %d", len(data), expected)
	}
	c := s.current.Load().active
	off := 9
	for i := 0; i < cmsDepth; i++ {
		for j := 0; j < wordsPerRow; j++ {
			w := binary.LittleEndian.Uint64(data[off:])
			atomic.StoreUint64(&c.rows[i][j], w)
			off += 8
		}
	}
	return nil
}

// Close stops background goroutines and waits for them to exit before
// returning. This join is critical when persistFn captures resources
// owned by the caller (e.g. RocksDB WriteOptions) that will be torn
// down immediately after Close returns — without the wait, the final
// Marshal+persistFn tick races the teardown. Not safe to call more
// than once.
func (s *Sketch) Close() error {
	if s.stopTimer != nil {
		close(s.stopTimer)
	}
	if s.stopPersist != nil {
		close(s.stopPersist)
	}
	s.bgWg.Wait()
	return nil
}

func (s *Sketch) timerLoop() {
	defer s.bgWg.Done()
	ticker := time.NewTicker(s.resetInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Reset()
		case <-s.stopTimer:
			return
		}
	}
}

func (s *Sketch) persistLoop() {
	defer s.bgWg.Done()
	ticker := time.NewTicker(s.persistInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if data := s.Marshal(); len(data) > 0 {
				s.persistFn(data)
			}
		case <-s.stopPersist:
			if data := s.Marshal(); len(data) > 0 {
				s.persistFn(data)
			}
			return
		}
	}
}

// ─── Count-Min Sketch (4-row, 4-bit counters, packed in uint64 words) ──

const (
	cmsDepth       = 4
	nibblesPerWord = 16 // 16 × 4-bit counters per uint64 word
	nibbleMask     = uint64(0xf)

	fnvOffsetBasis = 14695981039346656037
	fnvPrime       = 1099511628211

	// sketchFormatVersion is the first byte of Marshal output. The legacy
	// (pre-lock-free) format started with the low byte of `width`, which
	// is effectively never 0x02 for any realistic counter count — so 0x02
	// serves as an unambiguous sentinel to distinguish the two layouts.
	sketchFormatVersion byte = 0x02
)

// cms is a packed Count-Min Sketch with no synchronisation primitives of
// its own. Touch increments via atomic CAS on 4-bit nibbles; Estimate reads
// via atomic.LoadUint64. Concurrent access is safe without mutexes.
type cms struct {
	rows  [cmsDepth][]uint64
	width uint64 // counter count per row (power of 2)
	seeds [cmsDepth]uint64
}

func newCms(width uint64, seeds [cmsDepth]uint64) *cms {
	wordsPerRow := (width + nibblesPerWord - 1) / nibblesPerWord
	c := &cms{width: width, seeds: seeds}
	for i := range c.rows {
		c.rows[i] = make([]uint64, wordsPerRow)
	}
	return c
}

func (c *cms) increment(key []byte) {
	for i := 0; i < cmsDepth; i++ {
		idx := c.index(key, i)
		c.inc4bit(i, idx)
	}
}

func (c *cms) estimate(key []byte) int {
	min := math.MaxInt
	for i := 0; i < cmsDepth; i++ {
		idx := c.index(key, i)
		v := c.get4bit(i, idx)
		if int(v) < min {
			min = int(v)
		}
	}
	return min
}

// inc4bit performs a saturating +1 on a single 4-bit nibble using CAS on
// the containing uint64 word. Retries on contention; returns early when
// the nibble has reached the 4-bit ceiling (15).
func (c *cms) inc4bit(row int, idx uint64) {
	wordIdx := idx / nibblesPerWord
	shift := (idx % nibblesPerWord) * 4
	mask := nibbleMask << shift
	p := &c.rows[row][wordIdx]
	for {
		w := atomic.LoadUint64(p)
		val := (w & mask) >> shift
		if val >= 15 {
			return // saturated
		}
		newW := (w &^ mask) | ((val + 1) << shift)
		if atomic.CompareAndSwapUint64(p, w, newW) {
			return
		}
		// CAS failed: another goroutine modified the word; retry.
	}
}

func (c *cms) get4bit(row int, idx uint64) uint8 {
	wordIdx := idx / nibblesPerWord
	shift := (idx % nibblesPerWord) * 4
	w := atomic.LoadUint64(&c.rows[row][wordIdx])
	return uint8((w >> shift) & nibbleMask)
}

// index derives a row-specific bucket for the given key via FNV-1a with
// row-seeded starting state. Eats the full key []byte — no upstream
// projection. Deterministic across restarts because seeds are constants.
func (c *cms) index(key []byte, row int) uint64 {
	h := fnvOffsetBasis ^ c.seeds[row]
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime
	}
	return h & (c.width - 1) // fast mod (width is power of 2)
}

func nextPow2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}
