//go:build !no_rocksdb

package rocks

import (
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks/internal/native"
)

// getCFStats reads RocksDB property strings for the given column family.
func getCFStats(db *native.DB, cf int, name string) CFStats {
	get := func(prop string) string {
		v := db.Property(cf, prop)
		return strings.TrimSpace(v)
	}
	return CFStats{
		Name:        name,
		NumKeys:     get("rocksdb.estimate-num-keys"),
		DiskUsage:   get("rocksdb.estimate-live-data-size"),
		MemUsage:    get("rocksdb.cur-size-all-mem-tables"),
		Compactions: get("rocksdb.num-running-compactions"),
		BlobStats:   get("rocksdb.blob-stats"),
	}
}

// AllStats returns stats for all column families in the store.
func (s *impl) AllStats() []CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	var result []CFStats
	for name, cf := range s.cfh {
		if name == cfDefault {
			continue
		}
		result = append(result, getCFStats(s.db, cf, name))
	}
	return result
}
