//go:build !no_rocksdb

package rocks

import (
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

// diskBytes is shared by block-cache sizing and successful-fill aging.
func diskBytes(cfg runtime.RocksConfig) uint64 {
	n, _ := util.ParseSize(cfg.DiskBytes)
	if n == 0 {
		return 1 << 40 // 1 TiB default
	}
	return n
}

// buildConfig resolves only the settings used by this store. Native construction
// preserves their historical scope, including default-only write buffer sizes.
func buildConfig(cfg runtime.RocksConfig) native.Config {
	// BlockCache size = disk_bytes * mem_ratio.
	budget := diskBytes(cfg)
	memRatio := cfg.MemRatio
	if memRatio <= 0 {
		memRatio = 0.01
	}
	blockCacheBytes := uint64(float64(budget) * memRatio)
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
	// The legacy write-buffer setting applies only to the default metadata CF.
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
