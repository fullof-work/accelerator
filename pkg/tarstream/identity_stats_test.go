package tarstream

import (
	"bytes"
	"slices"
	"testing"
)

func TestPayloadExtentStats(t *testing.T) {
	for _, tc := range []struct {
		name            string
		extents         []extent
		boundary, total int64
		packed          int64
		dense           bool
		prefix          []extent
	}{
		{name: "empty", dense: true},
		{name: "all-hole-payload", boundary: 16384, total: 16384, dense: true},
		{name: "tail-only", extents: []extent{{0, 7}}, total: 7, dense: true},
		{name: "partial-page-boundary", extents: []extent{{0, 5018}}, boundary: 5001, total: 5018, packed: 5001, dense: true, prefix: []extent{{0, 5001}}},
		{name: "sparse-payload", extents: []extent{{0, 4}, {4096, 8}, {8192, 13}}, boundary: 8192, total: 8205, packed: 12, dense: true, prefix: []extent{{0, 4}, {4096, 8}}},
		{name: "adjacent-tail-extents", extents: []extent{{0, 4}, {8, 2}, {10, 3}}, boundary: 8, total: 13, packed: 4, dense: true, prefix: []extent{{0, 4}}},
		{name: "leading-tail-hole", extents: []extent{{9, 4}}, boundary: 8, total: 13},
		{name: "interior-tail-hole", extents: []extent{{8, 1}, {10, 3}}, boundary: 8, total: 13},
		{name: "trailing-tail-hole", extents: []extent{{8, 4}}, boundary: 8, total: 13},
		{name: "one-tib-boundary", extents: []extent{{(1 << 40) - 4096, 4109}}, boundary: 1 << 40, total: (1 << 40) + 13, packed: 4096, dense: true, prefix: []extent{{(1 << 40) - 4096, 4096}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packed, dense := payloadExtentStats(tc.extents, tc.boundary, tc.total)
			if dense != tc.dense || dense && packed != tc.packed {
				t.Fatalf("stats=(%d,%t), want (%d,%t)", packed, dense, tc.packed, tc.dense)
			}
			prefix, prefixPacked, prefixDense := prefixExtents(tc.extents, tc.boundary, tc.total)
			if prefixDense != dense || dense && (prefixPacked != packed || !slices.Equal(prefix, tc.prefix)) {
				t.Fatalf("legacy prefix=%v packed=%d dense=%t", prefix, prefixPacked, prefixDense)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				payloadExtentStats(tc.extents, tc.boundary, tc.total)
			}); allocations != 0 {
				t.Fatalf("extent statistics allocated %g times", allocations)
			}
		})
	}
}

func TestPresenceIdentityDoesNotAllocateForLegacyLayout(t *testing.T) {
	const count = 512
	const payload = count * 2 * 4096
	stored := make([]extent, count)
	for i := range stored {
		stored[i] = extent{int64(i * 2 * 4096), 4096}
	}
	presence := bytes.Repeat([]byte{0x55}, count/4)
	allocations := func(legacyLayout []extent) float64 {
		return testing.AllocsPerRun(5, func() {
			identity := newIdentityAccumulator(payload, legacyLayout, count*4096, 0, presence, stored)
			if identity.payloadRemaining != count*4096 {
				t.Fatalf("presence payload bytes: %d", identity.payloadRemaining)
			}
		})
	}
	withoutLegacy := allocations(nil)
	if withLegacy := allocations(stored); withLegacy != withoutLegacy {
		t.Fatalf("unused legacy layout increased allocations: empty=%g fragmented=%g", withoutLegacy, withLegacy)
	}
}
