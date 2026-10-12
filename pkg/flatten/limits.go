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

// PlanLimits bounds native construction, on-disk metadata, and Go indexes
// independently. The native process exits before the Go emitter starts.
// Zero fields select the documented defaults; exceeding a limit fails closed.
type PlanLimits struct {
	NativeMemoryBytes uint64
	PlanBytes         uint64
	IndexMemoryBytes  uint64
}

func (l PlanLimits) defaults() (PlanLimits, error) {
	if l.NativeMemoryBytes == 0 {
		l.NativeMemoryBytes = 512 << 20
	}
	if l.PlanBytes == 0 {
		l.PlanBytes = 512 << 20
	}
	if l.IndexMemoryBytes == 0 {
		l.IndexMemoryBytes = 128 << 20
	}
	if l.NativeMemoryBytes < 16<<20 || l.NativeMemoryBytes > math.MaxInt64 || l.PlanBytes < 192 || l.PlanBytes > math.MaxInt64 || l.IndexMemoryBytes < 4<<20 || l.IndexMemoryBytes > math.MaxInt64/4 {
		return l, fmt.Errorf("flatten: invalid plan budgets")
	}
	return l, nil
}
