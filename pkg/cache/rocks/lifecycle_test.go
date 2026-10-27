//go:build !no_rocksdb

package rocks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestPartitionsReopenReadOnly(t *testing.T) {
	cfg := tempRocksConfigT(t)
	fc := runtime.FreqConfig{DisableEviction: true, Counters: "1K"}
	s, err := openImpl(cfg, fc)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := store.ContentKey(sha256.Sum256([]byte("shared object and shard key")))
	parts := []store.Partition{store.PartitionChunk, store.PartitionManifest, store.PartitionBlob}
	for i, p := range parts {
		value := bytes.Repeat([]byte{byte(i + 1)}, 128+i*8192)
		shard := append([]byte{2, 5}, value...)
		if err := s.Fill(ctx, p, key, value); err != nil {
			t.Fatal(err)
		}
		if err := s.FillShard(ctx, p, key, shard); err != nil {
			t.Fatal(err)
		}
		if err := s.db.Flush(s.cfh[partitionToCF(p)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.compactAll(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, readonly := range []bool{true, false} {
		var reopened *impl
		if readonly {
			reopened, err = openReadOnlyImpl(cfg)
		} else {
			reopened, err = openImpl(cfg, fc)
		}
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range parts {
			value := bytes.Repeat([]byte{byte(i + 1)}, 128+i*8192)
			res, blob, err := reopened.Get(ctx, p, key)
			if err != nil || res != cache.CacheHit {
				t.Fatalf("object: %v %v", res, err)
			}
			if !bytes.Equal(blob.Bytes(), value) {
				t.Fatal("object changed")
			}
			blob.Release()
			res, blob, err = reopened.GetShard(ctx, p, key)
			if err != nil || res != cache.CacheHit {
				t.Fatalf("shard: %v %v", res, err)
			}
			if !bytes.Equal(blob.Bytes(), append([]byte{2, 5}, value...)) {
				t.Fatal("shard changed")
			}
			blob.Release()
			if readonly {
				if err := reopened.Fill(ctx, p, key, nil); !errors.Is(err, ErrReadOnly) {
					t.Fatalf("read-only Fill: %v", err)
				}
				if err := reopened.FillShard(ctx, p, key, nil); !errors.Is(err, ErrReadOnly) {
					t.Fatalf("read-only FillShard: %v", err)
				}
			}
		}
		if len(reopened.AllStats()) != 3 {
			t.Fatal("missing CF stats")
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentRequestsAndClose(t *testing.T) {
	s, err := openImpl(tempRocksConfigT(t), runtime.FreqConfig{
		Counters: "1K", DisableEviction: true, PersistInterval: "1ms", ResetInterval: "1ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var workers sync.WaitGroup
	started := make(chan struct{}, 8)
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			key := store.ContentKey(sha256.Sum256([]byte(fmt.Sprint(worker))))
			value := bytes.Repeat([]byte{byte(worker)}, 8192)
			started <- struct{}{}
			for {
				err := s.Fill(ctx, store.PartitionChunk, key, value)
				if errors.Is(err, errClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				res, blob, err := s.Get(ctx, store.PartitionChunk, key)
				if errors.Is(err, errClosed) {
					return
				}
				if err != nil || res != cache.CacheHit {
					t.Errorf("get: %v %v", res, err)
					return
				}
				if !bytes.Equal(blob.Bytes(), value) {
					t.Error("concurrent value corrupted")
				}
				blob.Release()
				s.AllStats()
			}
		}(worker)
	}
	for i := 0; i < 8; i++ {
		<-started
	}
	var closers sync.WaitGroup
	for i := 0; i < 4; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	closers.Wait()
	workers.Wait()
	if _, _, err := s.get(ctx, cfChunk, []byte("missing")); !errors.Is(err, errClosed) {
		t.Fatalf("closed read: %v", err)
	}
	if len(s.AllStats()) != 0 {
		t.Fatal("closed stats accessed native DB")
	}
}

func TestCloseWaitsForCompactionCallback(t *testing.T) {
	cfg := tempRocksConfigT(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	db, err := native.Open(cfg.Path, buildConfig(cfg), false, func([]byte) bool {
		once.Do(func() { close(entered) })
		<-release
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	s := &impl{db: db, cfh: cfIndexes()}
	defer s.Close()
	defer unblock() // Release a blocked callback even if fixture setup fails.
	// Four overlapping L0 files trigger automatic background compaction.
	// No store request holds the lifecycle lock once Flush returns, so Close
	// must rely on the native DB's background join while keeping the handle alive.
	for i := 0; i < 4; i++ {
		// Two user keys force overlapping ranges. Single-key SSTs can be
		// trivially moved without invoking any compaction filter.
		for _, key := range []string{"a", "z"} {
			if err := db.Put(native.Chunk, []byte(key), bytes.Repeat([]byte{byte(i)}, 8192)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(native.Chunk); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		unblock()
		t.Log(db.Property(native.Chunk, "rocksdb.cfstats"))
		t.Fatal("no callback")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		unblock()
		t.Fatalf("Close returned during callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestFilterInlineBlobAndPanic(t *testing.T) {
	for _, panicCallback := range []bool{false, true} {
		t.Run(fmt.Sprint(panicCallback), func(t *testing.T) {
			cfg := tempRocksConfigT(t)
			db, err := native.Open(cfg.Path, buildConfig(cfg), false, func(key []byte) bool {
				if panicCallback {
					panic("intentional test panic")
				}
				return key[0] == 0
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			control := []byte("default metadata must never be filtered")
			if err := db.Put(native.Default, []byte("control"), control); err != nil {
				t.Fatal(err)
			}
			if err := db.Flush(native.Default); err != nil {
				t.Fatal(err)
			}
			if err := db.Compact(native.Default); err != nil {
				t.Fatal(err)
			}
			var restored []byte
			found, err := db.Read(native.Default, []byte("control"), func(n int) []byte { restored = make([]byte, n); return restored })
			if err != nil || !found || !bytes.Equal(restored, control) {
				t.Fatalf("default CF was filtered: %v", err)
			}
			for _, cf := range []int{native.Chunk, native.Manifest, native.Blob} {
				for _, size := range []int{128, 8192} {
					for _, cold := range []byte{0, 1} {
						if err := db.Put(cf, []byte{cold, byte(size >> 8)}, bytes.Repeat([]byte{7}, size)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := db.Flush(cf); err != nil {
					t.Fatal(err)
				}
				if err := db.Compact(cf); err != nil {
					t.Fatal(err)
				}
				for _, size := range []int{128, 8192} {
					for _, cold := range []byte{0, 1} {
						var data []byte
						found, err := db.Read(cf, []byte{cold, byte(size >> 8)}, func(n int) []byte { data = make([]byte, n); return data })
						want := panicCallback || cold == 1
						if err != nil || found != want {
							t.Fatalf("cf=%d size=%d cold=%d: found=%v err=%v", cf, size, cold, found, err)
						}
						if found && !bytes.Equal(data, bytes.Repeat([]byte{7}, size)) {
							t.Fatal("value changed")
						}
					}
				}
			}
		})
	}
}

func TestOpenFailureAndRepeatedClose(t *testing.T) {
	cfg := tempRocksConfigT(t)
	fc := runtime.FreqConfig{Counters: "1K"}
	// A non-directory path fails before CF construction.
	bad := filepath.Join(cfg.Path, "file")
	if err := os.WriteFile(bad, []byte("not a DB"), 0600); err != nil {
		t.Fatal(err)
	}
	badCfg := cfg
	badCfg.Path = bad
	if _, err := openImpl(badCfg, fc); err == nil {
		t.Fatal("opened file as DB")
	}
	missing := cfg
	missing.Path = filepath.Join(cfg.Path, "absent")
	if _, err := openReadOnlyImpl(missing); err == nil {
		t.Fatal("read-only created DB")
	}
	var baseline int
	for i := 0; i < 25; i++ {
		s, err := openImpl(cfg, fc)
		if err != nil {
			t.Fatal(err)
		}
		// Lock failure happens after options/filter creation; all must unwind.
		if _, err := openImpl(cfg, fc); err == nil {
			t.Fatal("opened locked DB")
		}
		res, b, err := s.Get(context.Background(), store.PartitionChunk, store.ContentKey{})
		if err != nil || res != cache.CacheMiss || b != nil {
			t.Fatalf("missing: %v %v", res, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		fds, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			baseline = len(fds)
		}
		if len(fds) > baseline+2 {
			t.Fatalf("file descriptors leaked: %d -> %d", baseline, len(fds))
		}
	}
}

func TestGetStorageErrorIsNotMiss(t *testing.T) {
	cfg := tempRocksConfigT(t)
	s, err := openImpl(cfg, runtime.FreqConfig{Counters: "1K", DisableEviction: true})
	if err != nil {
		t.Fatal(err)
	}
	key := store.ContentKey(sha256.Sum256([]byte("blob corruption")))
	if err := s.Fill(context.Background(), store.PartitionChunk, key, bytes.Repeat([]byte{7}, 8192)); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(native.Chunk); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(cfg.Path, "*.blob"))
	if err != nil || len(files) == 0 {
		t.Fatal("no blob file")
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(bytes.Repeat([]byte{0xff}, 128), 128)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err = openReadOnlyImpl(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Get(context.Background(), store.PartitionChunk, key); err == nil {
		t.Fatal("corruption became cache miss/success")
	}
}

func TestBlobFilteringAvoidsValueReads(t *testing.T) {
	cfg := tempRocksConfigT(t)
	nativeCfg := buildConfig(cfg)
	nativeCfg.TrackBlobIO = true
	db, err := native.Open(cfg.Path, nativeCfg, false, func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const count = 32
	value := bytes.Repeat([]byte{0xab}, 32<<10)
	for i := 0; i < count; i++ {
		key := sha256.Sum256([]byte(fmt.Sprint(i)))
		if err := db.Put(native.Blob, key[:], value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(native.Blob); err != nil {
		t.Fatal(err)
	}
	// Prove the counter is wired by reading a real blob before measuring.
	key := sha256.Sum256([]byte("0"))
	beforeRead := db.BlobReadBytes()
	found, err := db.Read(native.Blob, key[:], func(n int) []byte { return make([]byte, n) })
	if err != nil || !found || db.BlobReadBytes() <= beforeRead {
		t.Fatalf("blob counter inactive: found=%v err=%v", found, err)
	}
	beforeCompact := db.BlobReadBytes()
	if err := db.Compact(native.Blob); err != nil {
		t.Fatal(err)
	}
	if delta := db.BlobReadBytes() - beforeCompact; delta != 0 {
		t.Fatalf("filter fetched %d unnecessary blob bytes", delta)
	}
	for i := 0; i < count; i++ {
		key := sha256.Sum256([]byte(fmt.Sprint(i)))
		found, err := db.Read(native.Blob, key[:], func(n int) []byte { return make([]byte, n) })
		if err != nil || found {
			t.Fatalf("cold key %d survived: %v", i, err)
		}
	}
	t.Logf("compacted %d x %d-byte blobs; engine blob-read delta = 0", count, len(value))
}

func TestBlobGCRelocatesLiveValues(t *testing.T) {
	cfg := tempRocksConfigT(t)
	s, err := openImpl(cfg, runtime.FreqConfig{Counters: "1K", DisableEviction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	// Keep one live value in the oldest blob file, then overwrite its neighbor.
	// Later files make that oldest file eligible under the unchanged GC cutoff.
	liveKey := []byte("old-live")
	overwriteKey := []byte("overwritten")
	value := bytes.Repeat([]byte{0x5a}, 16<<10)
	if err := s.put(ctx, cfChunk, liveKey, value); err != nil {
		t.Fatal(err)
	}
	if err := s.put(ctx, cfChunk, overwriteKey, value); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(native.Chunk); err != nil {
		t.Fatal(err)
	}
	initial, _ := filepath.Glob(filepath.Join(cfg.Path, "*.blob"))
	if len(initial) != 1 {
		t.Fatalf("initial blob files: %v", initial)
	}
	for i := 0; i < 8; i++ {
		if err := s.put(ctx, cfChunk, overwriteKey, bytes.Repeat([]byte{byte(i)}, len(value))); err != nil {
			t.Fatal(err)
		}
		if err := s.db.Flush(native.Chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.compactAll(); err != nil {
		t.Fatal(err)
	}
	got, err := rawRead(s, cfChunk, liveKey)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("GC live value: %v", err)
	}
	got, err = rawRead(s, cfChunk, overwriteKey)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{7}, len(value))) {
		t.Fatalf("GC latest value: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = os.Stat(initial[0])
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("oldest blob file was not reclaimed after relocating its live value")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := openReadOnlyImpl(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	got, err = rawRead(ro, cfChunk, liveKey)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("GC reopen: %v", err)
	}
}

// A pin must remain alive through allocation/copy, while Close waits at the
// store gate. The returned Go blob remains valid after native teardown.
func TestCloseWaitsForPinnedCopy(t *testing.T) {
	cfg := tempRocksConfigT(t)
	s, err := openImpl(cfg, runtime.FreqConfig{Counters: "1K", DisableEviction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := store.ContentKey(sha256.Sum256([]byte("pinned copy")))
	value := bytes.Repeat([]byte{3}, 8192)
	if err := s.Fill(ctx, store.PartitionChunk, key, value); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	original := chunkPool
	chunkPool = blockingPool{original, entered, release}
	defer func() { chunkPool = original }()
	type result struct {
		blob cache.Blob
		err  error
	}
	read := make(chan result, 1)
	go func() { _, blob, err := s.Get(ctx, store.PartitionChunk, key); read <- result{blob, err} }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		close(release)
		t.Fatalf("Close returned with an active pin: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	r := <-read
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.blob.Release()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.blob.Bytes(), value) {
		t.Fatal("Go blob changed after native shutdown")
	}
}

type blockingPool struct {
	pool    cache.BlobPool
	entered chan struct{}
	release chan struct{}
}

func (p blockingPool) Alloc(n int) ([]byte, cache.Blob) {
	close(p.entered)
	<-p.release
	return p.pool.Alloc(n)
}

// The former binding passed the Go integer as a double. Let RocksDB apply its
// existing 100-bit clamp instead of truncating a large setting to a C int.
func TestBloomBitsPreserveEngineClamping(t *testing.T) {
	cfg := tempRocksConfigT(t)
	large := uint64(1)<<32 | 15
	cfg.BloomBits = int(large)
	if uint64(cfg.BloomBits) != large {
		t.Skip("requires a 64-bit config integer")
	}
	s, err := openImpl(cfg, runtime.FreqConfig{Counters: "1K", DisableEviction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	paths, err := filepath.Glob(filepath.Join(cfg.Path, "OPTIONS-*"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("options files: %v %v", paths, err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("filter_policy=bloomfilter:100:false")) {
			return
		}
	}
	t.Fatal("Bloom setting was truncated before the engine applied its clamp")
}
