//go:build !no_rocksdb

package rocks

import (
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/freq"
)

// sketchCompactionFilter drives RocksDB compaction on all three data
// CFs based on the frequency sketch owned by the Store. Keys
// whose sketch estimate is at or below the configured evict threshold
// are removed; everything else is preserved.
//
// Two-phase initialisation:
//  1. newSketchCompactionFilter() — sketch=nil, armed=false. Attach to
//     CFs and open the DB; any startup/recovery compaction sees
//     armed=false and preserves everything unconditionally.
//  2. SetSketch(s) after the sketch is constructed, Arm() after the
//     sketch is restored from CF_DEFAULT. Evict() now consults the
//     sketch.
//
// Thread safety: RocksDB invokes Evict from multiple compaction
// threads concurrently. sketch is an atomic.Pointer and the underlying
// freq.Sketch is lock-free; armed is an atomic.Bool. No additional
// synchronisation is required.
//
// The filter never calls sketch.Touch — compaction happens independently
// of user access, so treating it as an access event would make the
// sketch self-reinforce (every compaction would refresh every key).
// Evict is strictly read-only against the sketch.
type sketchCompactionFilter struct {
	sketch atomic.Pointer[freq.Sketch]
	armed  atomic.Bool
}

func newSketchCompactionFilter() *sketchCompactionFilter {
	return &sketchCompactionFilter{}
}

// Evict decides both inline and blob filtering using only the key.
// The borrowed key is used synchronously and is never retained.
func (f *sketchCompactionFilter) Evict(key []byte) bool {
	if !f.armed.Load() {
		return false
	}
	s := f.sketch.Load()
	if s == nil {
		return false
	}
	return s.ShouldEvict(key)
}

// SetSketch installs the sketch pointer. Must be called exactly once
// from rocks.Open, after the Store is fully constructed.
func (f *sketchCompactionFilter) SetSketch(s *freq.Sketch) { f.sketch.Store(s) }

// Arm opens the gate. After Arm, Evict consults the sketch instead of
// unconditionally preserving. Must be called exactly once from
// rocks.Open, after SetSketch and after the persisted sketch has been
// restored from CF_DEFAULT.
func (f *sketchCompactionFilter) Arm() { f.armed.Store(true) }
