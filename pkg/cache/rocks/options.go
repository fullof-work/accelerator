//go:build !no_rocksdb

package rocks

import (
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

// buildConfig resolves only the settings used by this store. Native construction
// preserves their historical scope, including default-only write buffer sizes.
func buildConfig(cfg runtime.RocksConfig) native.Config {
	// BlockCache size = disk_bytes * mem_ratio
	diskBytes, _ := util.ParseSize(cfg.DiskBytes)
	if diskBytes == 0 {
		diskBytes = 1 << 40 // 1 TiB default
	}
	memRatio := cfg.MemRatio
	if memRatio <= 0 {
		memRatio = 0.01
	}
	blockCacheBytes := uint64(float64(diskBytes) * memRatio)
	if blockCacheBytes < 64<<20 {
		blockCacheBytes = 64 << 20 // min 64 MiB
	}

	// Block size
	blockSize, _ := util.ParseSize(cfg.BlockSize)
	if blockSize == 0 {
		blockSize = 64 << 10 // 64 KiB
	}

	// Bloom filter
	bloomBits := cfg.BloomBits
	if bloomBits <= 0 {
		bloomBits = 15
	}
	// Write buffer — default 256 MiB to reduce flush frequency under concurrent writes.
	writeBufferBytes, _ := util.ParseSize(cfg.WriteBufferBytes)
	if writeBufferBytes == 0 {
		writeBufferBytes = 256 << 20 // 256 MiB default (was 64 MiB Go/RocksDB default)
	}

	// Background jobs
	maxJobs := cfg.MaxBackgroundJobs
	if maxJobs <= 0 {
		maxJobs = 8 // default 8 (was 4)
	}
	return native.Config{
		CacheBytes:            blockCacheBytes,
		BlockBytes:            blockSize,
		WriteBufferBytes:      writeBufferBytes,
		BloomBits:             bloomBits,
		BackgroundCompactions: maxJobs,
		DirectReads:           runtime.BoolDefault(cfg.DirectReads, true),
	}
}
