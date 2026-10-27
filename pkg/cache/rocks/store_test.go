//go:build !no_rocksdb

package rocks

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/freq"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// tempRocksConfigT is the testing.T flavour of tempRocksConfig (which
// store_bench_test.go provides for *testing.B). Kept local to avoid
// entangling the two files.
func tempRocksConfigT(t *testing.T) runtime.RocksConfig {
	t.Helper()
	return runtime.RocksConfig{
		Path:      t.TempDir(),
		DiskBytes: "64MiB",
		MemRatio:  0.1,
		BloomBits: 10,
	}
}

// TestBlobCF proves the blob partition gets its own column family
// (cfBlob), round-trips through the public Get/Fill, and is isolated
// from the chunk CF — i.e. partitionToCF(PartitionBlob) == cfBlob.
func TestBlobCF(t *testing.T) {
	s, err := openImpl(tempRocksConfigT(t), runtime.FreqConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	k := store.ContentKey(sha256.Sum256([]byte("blob-cf-test")))
	data := []byte("blob value in its own CF")

	if err := s.Fill(ctx, store.PartitionBlob, k, data); err != nil {
		t.Fatalf("Fill(blob): %v", err)
	}
	res, blob, err := s.Get(ctx, store.PartitionBlob, k)
	if err != nil || res != cache.CacheHit {
		t.Fatalf("Get(blob): res=%v err=%v", res, err)
	}
	if string(blob.Bytes()) != string(data) {
		t.Fatalf("Get(blob): got %q want %q", blob.Bytes(), data)
	}
	blob.Release()

	// Isolation: the same key under chunk is a miss (separate CF).
	if res2, _, _ := s.Get(ctx, store.PartitionChunk, k); res2 == cache.CacheHit {
		t.Fatal("blob Fill must not be visible under the chunk CF")
	}
	// The blob CF must actually have been opened.
	if _, ok := s.cfh[cfBlob]; !ok {
		t.Fatal("cfBlob column family was not opened")
	}
}

// TestPersistFnRoundtrip proves that freq.Sketch's tick-path persist
// actually runs when FreqConfig.PersistInterval > 0. The test is a
// regression guard: before Phase 2, buildSketch silently dropped the
// PersistFn field, so persistLoop never started and a Put → kill -9 →
// reopen cycle lost the entire sketch.
func TestPersistFnRoundtrip(t *testing.T) {
	cfg := tempRocksConfigT(t)

	// First open: 50ms tick interval + default counters. Counters must be
	// stable across the close/reopen cycle — rely on defaults.
	freqCfg := runtime.FreqConfig{
		PersistInterval: "50ms",
	}
	s1, err := openImpl(cfg, freqCfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Put + Touch a few keys. Each Put calls sketch.Touch internally.
	ctx := context.Background()
	keys := make([][]byte, 5)
	for i := range keys {
		h := sha256.Sum256([]byte(fmt.Sprintf("persist-key-%d", i)))
		keys[i] = h[:]
		if err := s1.put(ctx, cfChunk, keys[i], []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Poll for the persisted blob to appear. The 50ms ticker should
	// fire almost immediately under normal load, but under -race the
	// goroutine scheduler slows down by ~10x — a fixed sleep of 250ms
	// races against persistLoop's first tick. Poll up to 5 seconds.
	deadline := time.Now().Add(5 * time.Second)
	var size int
	var firstByte byte
	for time.Now().Before(deadline) {
		slice, err := rawRead(s1, cfDefault, []byte(freq.ReservedSketchKey))
		if err != nil {
			t.Fatalf("GetCF(ReservedSketchKey): %v", err)
		}
		if slice != nil {
			size = len(slice)
			if size > 0 {
				firstByte = slice[0]
			}
		}
		if size > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if size == 0 {
		t.Fatal("sketch was not persisted by tick-path within 5s — PersistFn wiring is broken")
	}
	if firstByte != 0x02 {
		t.Fatalf("persisted sketch version byte: got 0x%02x, want 0x02", firstByte)
	}

	// Close flushes PersistSketch synchronously on top of whatever the
	// tick-path already wrote — the two paths are idempotent.
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and assert the frequency data survived the round-trip.
	// Each key should have been Touch-ed exactly once (via Put); after
	// Unmarshal, Estimate(key) must be ≥ 1.
	s2, err := openImpl(cfg, runtime.FreqConfig{}) // no tick-path on reopen
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	defer s2.Close()

	for i, k := range keys {
		est := s2.Sketch().Estimate(k)
		if est == 0 {
			t.Errorf("key[%d]: Estimate after reopen = 0, want ≥ 1 (sketch did not persist)", i)
		}
	}
}

// TestCompactionFilterEvictsColdKeys is the end-to-end proof that
// sketchCompactionFilter is wired to both the chunk CF and the manifest
// CF, and that cold keys are removed during compaction while hot keys
// are preserved. This is the reason Phase 2 exists.
//
// Stability notes:
//   - EvictThreshold is set to 5 so that a cold key (Touch count = 1
//     from Put) is evictable (1 ≤ 5) while a hot key (Touch count ≥ 6)
//     saturates toward the 4-bit ceiling (15) and is preserved (15 > 5).
//   - A single Get is enough per hit to Touch the sketch; 10 Gets leaves
//     comfortable margin above the threshold, well inside the 4-bit
//     ceiling.
//   - Flush moves memtable state into an L0 SST (CompactionFilter never
//     sees memtable keys), CompactRangeCF is synchronous, so no polling
//     is needed.
func TestCompactionFilterEvictsColdKeys(t *testing.T) {
	cfg := tempRocksConfigT(t)
	freqCfg := runtime.FreqConfig{
		EvictThreshold: 5,
		ResetAfter:     "1G", // effectively never auto-resets inside the test
	}
	s, err := openImpl(cfg, freqCfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Test matrix: one cold + one hot key per CF, same pattern in both.
	type kv struct {
		ns  string
		key []byte
		val []byte
	}
	mk := func(ns, seed string) kv {
		h := sha256.Sum256([]byte(ns + ":" + seed))
		return kv{ns: ns, key: h[:], val: []byte("value-" + seed)}
	}
	entries := []struct {
		name string
		kv   kv
		hot  bool
	}{
		{"chunk-cold", mk(cfChunk, "cold"), false},
		{"chunk-hot", mk(cfChunk, "hot"), true},
		{"manifest-cold", mk(cfManifest, "cold"), false},
		{"manifest-hot", mk(cfManifest, "hot"), true},
	}

	// Seed: Put all four keys. Each Put → one Touch.
	ctx := context.Background()
	for _, e := range entries {
		if err := s.put(ctx, e.kv.ns, e.kv.key, e.kv.val); err != nil {
			t.Fatalf("%s Put: %v", e.name, err)
		}
	}

	// Heat the "hot" keys above the threshold. 10 Gets each puts the
	// nibble at 11 — well above threshold=5 and far from the 4-bit
	// saturation ceiling.
	for _, e := range entries {
		if !e.hot {
			continue
		}
		for i := 0; i < 10; i++ {
			blob, ok, err := s.get(ctx, e.kv.ns, e.kv.key)
			if err != nil {
				t.Fatalf("%s Get: %v", e.name, err)
			}
			if !ok {
				t.Fatalf("%s Get: unexpected miss during heating", e.name)
			}
			blob.Release()
		}
	}

	// Force memtable → L0 SST for both CFs so compaction actually runs
	// over the data (CompactionFilter does not see memtable entries).
	for _, cfName := range []string{cfChunk, cfManifest} {
		if err := s.db.Flush(s.cfh[cfName]); err != nil {
			t.Fatalf("FlushCF %s: %v", cfName, err)
		}
	}

	// Synchronous manual compaction on both data CFs. This invokes
	// sketchCompactionFilter.Filter() on every key.
	if err := s.compactAll(); err != nil {
		t.Fatal(err)
	}

	// Verify: cold keys gone, hot keys preserved. Don't use s.Get here
	// because Get itself Touches the sketch; read through db.GetCF so
	// the post-compaction state is observed untouched.
	for _, e := range entries {
		slice, err := rawRead(s, e.kv.ns, e.kv.key)
		if err != nil {
			t.Fatalf("%s post-compact GetCF: %v", e.name, err)
		}
		present := slice != nil && len(slice) > 0
		if e.hot && !present {
			t.Errorf("%s: hot key was evicted by CompactionFilter (expected preserved)", e.name)
		}
		if !e.hot && present {
			t.Errorf("%s: cold key survived CompactionFilter (expected evicted)", e.name)
		}
	}
}

// TestCompactionFilterUnarmedPreserves guards the recovery-window
// invariant: before rocks.Open finishes restoring the sketch, the filter
// MUST preserve every key unconditionally — otherwise any
// startup/recovery compaction triggered inside OpenDbColumnFamilies
// could drop live data against a nil or empty sketch. Pure Go test,
// no DB needed.
func TestCompactionFilterUnarmedPreserves(t *testing.T) {
	f := newSketchCompactionFilter()

	// State 1: sketch=nil, armed=false. Every key is preserved.
	if remove := f.Evict([]byte("any-key")); remove {
		t.Fatal("filter with sketch=nil, armed=false removed a key (recovery-window invariant broken)")
	}

	// State 2: sketch=set, armed=false. Still preserved — Arm is the
	// gate, not SetSketch. Use a sketch whose threshold would otherwise
	// remove the key.
	s := freq.New(freq.Config{EvictThreshold: 15})
	f.SetSketch(s)
	defer s.Close()
	if remove := f.Evict([]byte("any-key")); remove {
		t.Fatal("filter with sketch=set, armed=false removed a key (Arm gate bypassed)")
	}

	// State 3: armed. Now the sketch decides. A never-Touch-ed key has
	// Estimate = 0, threshold = 15, so ShouldEvict = true.
	f.Arm()
	if remove := f.Evict([]byte("any-key")); !remove {
		t.Fatal("filter with armed=true and cold key failed to remove (armed gate not effective)")
	}
}

// TestBlobDBEnabled is the end-to-end proof that chunk and manifest CFs
// are actually routing large values through RocksDB's BlobDB — the
// Phase 1 doc promised it, Phase 1 code forgot to wire it up, and
// Step 1 of the current change finally hooks EnableBlobFiles onto
// both CF Options. The test Put-s values larger than the 4 KiB
// min_blob_size threshold on both CFs, flushes memtable to L0, and
// asserts that at least one .blob file shows up on disk.
func TestBlobDBEnabled(t *testing.T) {
	cfg := tempRocksConfigT(t)
	s, err := openImpl(cfg, runtime.FreqConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// 16 KiB value comfortably above the 4 KiB min_blob_size gate.
	value := make([]byte, 16*1024)
	for i := range value {
		value[i] = byte(i)
	}
	ctx := context.Background()
	chunkKey := sha256.Sum256([]byte("blob-chunk-key"))
	manifestKey := sha256.Sum256([]byte("blob-manifest-key"))
	if err := s.put(ctx, cfChunk, chunkKey[:], value); err != nil {
		t.Fatalf("Put chunk: %v", err)
	}
	if err := s.put(ctx, cfManifest, manifestKey[:], value); err != nil {
		t.Fatalf("Put manifest: %v", err)
	}

	// Force memtable → L0 SST so BlobDB's SST-builder path runs.
	// Without this, values stay in memtable and no .blob file is
	// ever written.
	for _, cf := range []string{cfChunk, cfManifest} {
		if err := s.db.Flush(s.cfh[cf]); err != nil {
			t.Fatalf("FlushCF %s: %v", cf, err)
		}
	}

	// Count .blob files in the rocks directory.
	entries, err := os.ReadDir(cfg.Path)
	if err != nil {
		t.Fatalf("read rocks dir: %v", err)
	}
	var blobFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".blob") {
			blobFiles = append(blobFiles, e.Name())
		}
	}
	if len(blobFiles) == 0 {
		// List directory to help debug if it fires.
		var allNames []string
		for _, e := range entries {
			allNames = append(allNames, e.Name())
		}
		t.Fatalf("expected at least one .blob file after Flush; rocks dir contents: %v", allNames)
	}
	t.Logf("BlobDB produced %d .blob file(s) in %s", len(blobFiles), filepath.Base(cfg.Path))
}

// rawRead bypasses frequency accounting while still releasing pinned memory.
func rawRead(s *impl, ns string, key []byte) ([]byte, error) {
	var data []byte
	_, err := s.db.Read(s.cfh[ns], key, func(n int) []byte {
		data = make([]byte, n)
		return data
	})
	return data, err
}
