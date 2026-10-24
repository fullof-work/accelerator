package flatten

import (
	"fmt"
	"math"
)

// ArchiveLimits bounds the outer Docker-save archive and its member index.
// The same limits apply to seekable input and the single non-seekable spool.
type ArchiveLimits struct {
	Bytes      int64
	IndexBytes int64
	Members    int
}

func (l ArchiveLimits) defaults() (ArchiveLimits, error) {
	if l.Bytes == 0 {
		l.Bytes = 128 << 30
	}
	if l.IndexBytes == 0 {
		l.IndexBytes = 32 << 20
	}
	if l.Members == 0 {
		l.Members = 131072
	}
	if l.Bytes < 1024 || l.Bytes >= math.MaxInt64 || l.IndexBytes < 8<<20 || l.Members < 1 || l.Members > 1000000 {
		return l, fmt.Errorf("flatten: invalid archive limits")
	}
	return l, nil
}

// PlanLimits bounds controlled planning allocations, on-disk metadata, and
// reader/writer indexes independently. Planning drops its indexes before emission;
// Go GC and allocator overhead are process-wide and are not an RSS guarantee.
// Zero fields impose no extra admission quota; normalization retains only safe
// integer/allocation ceilings for the existing checked accounting. Positive
// values remain opt-in quotas. Callers must bound the execution environment.
type PlanLimits struct {
	// NativeMemoryBytes retains its name for compatibility. Half admits Go
	// source/dedup indexes and half admits go-erofs metadata/layout allocations.
	NativeMemoryBytes uint64
	PlanBytes         uint64
	IndexMemoryBytes  uint64
}

func (l PlanLimits) defaults() (PlanLimits, error) {
	if l.NativeMemoryBytes == 0 {
		l.NativeMemoryBytes = uint64(math.MaxInt)
	}
	if l.PlanBytes == 0 {
		l.PlanBytes = math.MaxInt64
	}
	if l.IndexMemoryBytes == 0 {
		// Leave headroom for shares, source-count charges and growth arithmetic.
		// This is a representation ceiling, not a workload-size policy.
		l.IndexMemoryBytes = uint64(math.MaxInt) / 4
	}
	if l.NativeMemoryBytes < 16<<20 || l.NativeMemoryBytes > math.MaxInt64 || l.PlanBytes < 192 || l.PlanBytes > math.MaxInt64 || l.IndexMemoryBytes < 4<<20 || l.IndexMemoryBytes > math.MaxInt64/4 {
		return l, fmt.Errorf("flatten: invalid plan budgets")
	}
	return l, nil
}
