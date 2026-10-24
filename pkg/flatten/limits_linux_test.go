//go:build linux

package flatten

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

func TestPlanLimitsZeroAndExplicitQuotas(t *testing.T) {
	got, err := (PlanLimits{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	if got.NativeMemoryBytes != uint64(math.MaxInt) || got.PlanBytes != math.MaxInt64 || got.IndexMemoryBytes != uint64(math.MaxInt)/4 {
		t.Fatalf("zero limits still impose workload quotas: %+v", got)
	}
	again, err := got.defaults()
	if err != nil || again != got {
		t.Fatalf("normalization not idempotent: %+v, %v", again, err)
	}
	explicit := PlanLimits{NativeMemoryBytes: 512 << 20, PlanBytes: 512 << 20, IndexMemoryBytes: 128 << 20}
	got, err = explicit.defaults()
	if err != nil || got != explicit {
		t.Fatalf("explicit limits changed: %+v, %v", got, err)
	}
	mixed, err := (PlanLimits{PlanBytes: 512 << 20}).defaults()
	if err != nil || mixed.PlanBytes != 512<<20 || mixed.NativeMemoryBytes <= 512<<20 || mixed.IndexMemoryBytes <= 128<<20 {
		t.Fatalf("mixed limits: %+v, %v", mixed, err)
	}
	for _, bad := range []PlanLimits{
		{NativeMemoryBytes: 1}, {PlanBytes: 1}, {IndexMemoryBytes: 1},
		{NativeMemoryBytes: math.MaxUint64}, {PlanBytes: math.MaxUint64}, {IndexMemoryBytes: math.MaxUint64},
	} {
		if _, err := bad.defaults(); err == nil {
			t.Fatalf("invalid limit accepted: %+v", bad)
		}
	}
}

func TestDefaultPlanningAdmitsFormerDedupBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     uint64
		wantError bool
	}{
		{"default", 0, false}, {"explicit", 512 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := (PlanLimits{NativeMemoryBytes: tc.limit}).defaults()
			if err != nil {
				t.Fatal(err)
			}
			m := planningMemory{limit: l.NativeMemoryBytes / 2}
			if err = m.reserve(2 * planReadBatch); err != nil {
				t.Fatal(err)
			}
			table := planningTable[[32]byte, uint64]{hash: func(k [32]byte) uint64 { return binary.LittleEndian.Uint64(k[:8]) }, memory: &m}
			var input [8]byte
			const boundary = 1572864 // 6 GiB of unique 4 KiB blocks, without a 6 GiB fixture.
			for i := uint64(0); i <= boundary; i++ {
				binary.LittleEndian.PutUint64(input[:], i)
				key := sha256.Sum256(input[:])
				err = table.insert(context.Background(), key, i)
				if err != nil {
					if !tc.wantError || i != boundary || !strings.Contains(err.Error(), "memory budget exceeded") {
						t.Fatalf("block %d: %v", i, err)
					}
					return
				}
			}
			if tc.wantError {
				t.Fatal("explicit memory quota was ignored")
			}
			if table.count != boundary+1 {
				t.Fatalf("admitted %d blocks", table.count)
			}
		})
	}
}

func TestDefaultPlanSourceIndexBeyondFormerQuota(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     uint64
		wantError bool
	}{
		{"default", 0, false}, {"explicit", 128 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := (PlanLimits{IndexMemoryBytes: tc.limit}).defaults()
			if err != nil {
				t.Fatal(err)
			}
			g := goPlanner{limits: l, planBytes: planHeaderSize, sources: bufio.NewWriter(io.Discard)}
			const boundary = 112867
			for i := 0; i <= boundary; i++ {
				rel := strings.Repeat("p", 72) + fmt.Sprintf("%08d", i)
				err = g.addSource(rel, fileIdentity{size: 1, nlink: 1})
				if err != nil {
					if !tc.wantError || i != boundary || !strings.Contains(err.Error(), "index memory budget exceeded") {
						t.Fatalf("source %d: %v", i, err)
					}
					return
				}
			}
			if tc.wantError {
				t.Fatal("explicit index quota was ignored")
			}
		})
	}
}

func TestPlanSourceCountUsesRepresentationBound(t *testing.T) {
	l, err := (PlanLimits{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	g := goPlanner{limits: l, sourceCount: 1000000, sourceBytes: 1000000 * 85, planBytes: planHeaderSize + 1000000*85, sources: bufio.NewWriter(io.Discard)}
	if err = g.addSource("valid", fileIdentity{size: 1, nlink: 1}); err != nil {
		t.Fatalf("arbitrary million-source cap remains: %v", err)
	}
	// This count used to wrap count*256 before the sum comparison; it is
	// below the addressable slice bound on 64-bit hosts, so test the guard.
	if maxPlanSources > math.MaxUint64/256 {
		g.sourceCount = math.MaxUint64 / 256
		if err = g.addSource("valid", fileIdentity{size: 1, nlink: 1}); err == nil {
			t.Fatal("wrapped source-index charge accepted")
		}
	}
	g.sourceCount = maxPlanSources
	if err = g.addSource("valid", fileIdentity{size: 1, nlink: 1}); err == nil {
		t.Fatal("unrepresentable source table accepted")
	}
}

func TestPlanByteQuotaOptIn(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     uint64
		wantError bool
	}{
		{"default", 0, false}, {"explicit", 512 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := (PlanLimits{PlanBytes: tc.limit}).defaults()
			if err != nil {
				t.Fatal(err)
			}
			g := goPlanner{limits: l, planBytes: planHeaderSize}
			err = g.chargePlan(512 << 20)
			if (err != nil) != tc.wantError {
				t.Fatalf("quota admission: %v", err)
			}
			g.planBytes = l.PlanBytes
			if err = g.chargePlan(1); err == nil {
				t.Fatal("accounting ceiling exceeded")
			}
		})
	}
}
