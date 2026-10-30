//go:build !no_rocksdb

package rocks

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestFillTurnoverSharedByDataPartitionsAndShards(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.DiskBytes = "12" // Three four-byte payloads, independent of CF/key sizes.
	s, err := openImpl(cfg, runtime.FreqConfig{
		Counters: "1K", ResetAfter: "1G", DisableEviction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	cold := store.ContentKey(sha256.Sum256([]byte("turnover-cold")))
	if result, blob, err := s.Get(ctx, store.PartitionChunk, cold); err != nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("initial Get = %v, %v, %v", result, blob, err)
	}
	if err := s.Fill(ctx, store.PartitionChunk, cold, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if got := s.Sketch().Estimate(cold[:]); got != 2 {
		t.Fatalf("Get miss + Fill frequency = %d, want 2", got)
	}
	manifest := store.ContentKey(sha256.Sum256([]byte("turnover-manifest")))
	if err := s.Fill(ctx, store.PartitionManifest, manifest, []byte("meta")); err != nil {
		t.Fatal(err)
	}
	if got := s.Sketch().Estimate(cold[:]); got != 2 {
		t.Fatalf("rollover before twelve payload bytes: got %d, want 2", got)
	}
	blobKey := store.ContentKey(sha256.Sum256([]byte("turnover-shard")))
	if err := s.FillShard(ctx, store.PartitionBlob, blobKey, []byte{0, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if got := s.Sketch().Estimate(cold[:]); got != 1 {
		t.Fatalf("shared byte budget did not age cold history: got %d, want 1", got)
	}
	// Aging only changes frequency. It does not synchronously delete data,
	// and disable_eviction continues to preserve the cached object.
	result, blob, err := s.Get(ctx, store.PartitionChunk, cold)
	if blob != nil {
		defer blob.Release()
	}
	if err != nil || result != cache.CacheHit || blob == nil || string(blob.Bytes()) != "data" {
		t.Fatalf("Fill rollover changed cached data: %v, %v", result, err)
	}
}

func TestFillTurnoverCountsOverwritesButNotEmptyOrFailedWrites(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.DiskBytes = "12"
	s, err := openImpl(cfg, runtime.FreqConfig{
		Counters: "1K", ResetAfter: "1G", DisableEviction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := store.ContentKey(sha256.Sum256([]byte("turnover-overwrite")))
	if _, _, err := s.Get(ctx, store.PartitionBlob, key); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.Fill(ctx, store.PartitionBlob, key, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Fill(ctx, store.PartitionBlob, key, nil); err != nil {
		t.Fatal(err)
	}
	if got := s.Sketch().Estimate(key[:]); got != 4 {
		t.Fatalf("empty write must Touch without aging: got %d, want 4", got)
	}
	if err := s.Fill(ctx, store.PartitionBlob, key, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if got := s.Sketch().Estimate(key[:]); got != 2 {
		t.Fatalf("successful overwrites did not advance bytes: got %d, want 2", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := s.Sketch().Estimate(key[:])
	if err := s.Fill(ctx, store.PartitionBlob, key, []byte("twelve bytes")); !errors.Is(err, errClosed) {
		t.Fatalf("closed Fill = %v, want errClosed", err)
	}
	if got := s.Sketch().Estimate(key[:]); got != before {
		t.Fatalf("failed Fill changed frequency: %d -> %d", before, got)
	}
}

func TestFillTurnoverCheckpointAndReopen(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.DiskBytes = "12"
	fc := runtime.FreqConfig{Counters: "1K", ResetAfter: "1G", DisableEviction: true}
	s, err := openImpl(cfg, fc)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := store.ContentKey(sha256.Sum256([]byte("turnover-checkpoint")))
	if _, _, err := s.Get(ctx, store.PartitionChunk, key); err != nil {
		t.Fatal(err)
	}
	if err := s.Fill(ctx, store.PartitionChunk, key, []byte("12345678")); err != nil {
		t.Fatal(err)
	}
	s.PersistSketch() // Much larger than the byte budget; must not count as Fill.
	if got := s.Sketch().Estimate(key[:]); got != 2 {
		t.Fatalf("checkpoint advanced byte aging: got %d, want 2", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openImpl(cfg, fc)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Sketch().Estimate(key[:]); got != 2 {
		t.Fatalf("checkpoint frequency not restored: got %d, want 2", got)
	}
	if err := reopened.Fill(ctx, store.PartitionChunk, key, []byte("next")); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Sketch().Estimate(key[:]); got != 3 {
		t.Fatalf("reopen retained old byte progress: got %d, want 3", got)
	}
}
