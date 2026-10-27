package tarstream

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestWriterIndexBudgetBeforeOutput(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 5*4096)
	r := &countingReaderAt{ReaderAt: bytes.NewReader(data)}
	src, err := sparse.NewSource(r, uint64(len(data)), []sparse.Extent{{Offset: 4096, Size: 4096}, {Offset: 3 * 4096, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	_, _, err = WriteTo(context.Background(), &output, "image", src, WithIndexMemoryLimit(2<<20+2*512))
	if err == nil || !strings.Contains(err.Error(), "index memory limit") {
		t.Fatal(err)
	}
	if output.Len() != 0 || r.read != 0 {
		t.Fatal("budget failure emitted or read payload")
	}
	var baseline bytes.Buffer
	if _, _, err = WriteTo(context.Background(), &baseline, "image", src); err != nil {
		t.Fatal(err)
	}
	if _, _, err = WriteTo(context.Background(), &output, "image", src, WithIndexMemoryLimit(4<<20)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), baseline.Bytes()) {
		t.Fatal("budget changed artifact identity")
	}
}

type extentLimitSource struct {
	size    uint64
	allZero bool
	reads   int
}

func (s *extentLimitSource) Size() uint64 { return s.size }
func (s *extentLimitSource) ReadAt(context.Context, []byte, uint64) (int, error) {
	s.reads++
	return 0, fmt.Errorf("unexpected payload read during preflight")
}
func (s *extentLimitSource) RunAt(off, limit uint64) (sparse.Run, error) {
	end, kind := min(s.size, off+limit), sparse.Zero
	if !s.allZero {
		end = min(end, (off/4096+1)*4096)
		kind = sparse.Data
		if off/4096%2 != 0 {
			kind = sparse.Hole
		}
	}
	return tarSourceRun[*extentLimitSource]{source: s, offset: off, end: end, kind: kind}, nil
}

func TestPresenceBudgetBeforeBitmapAndOutput(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		s := &extentLimitSource{size: maxPresencePayload, allZero: true}
		var output bytes.Buffer
		options := []WriteOption{WithIndexMemoryLimit(3 << 20)}
		if encrypted {
			options = append(options, WithCodec(&optionTestCodec{}, true))
		}
		_, _, err := WriteTo(context.Background(), &output, "disk", s, options...)
		if err == nil || !strings.Contains(err.Error(), "index memory limit") || output.Len() != 0 || s.reads != 0 {
			t.Fatal("presence budget must fail before output or data reads", encrypted, output.Len(), s.reads, err)
		}
	}
}

func TestPresenceOneTiBPlanFitsSingleBitmapBudget(t *testing.T) {
	// Enough for one 32 MiB bitmap and fixed writer state, not another bitmap
	// copy. Planning must not read or hash the 1 TiB logical payload.
	const budget = uint64(36 << 20)
	source := &presencePlanSource{size: maxPresencePayload, payload: maxPresencePayload, kind: sparse.Hole}
	plan, err := makeWritePlanLimited(context.Background(), "disk", source, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.presence) != 32<<20 || plan.packedSize != 0 || source.reads != 0 {
		t.Fatalf("unexpected bounded plan: bitmap=%d packed=%d reads=%d", len(plan.presence), plan.packedSize, source.reads)
	}
}

func TestWriterReaderSharedExtentLimit(t *testing.T) {
	// The sentinel counts even when the last real extent ends before EOF.
	s := &extentLimitSource{size: uint64(maxSparseExtents-1) * 8192}
	plan, err := makeWritePlan("disk", s)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.extents) != maxSparseExtents-1 {
		t.Fatal(len(plan.extents))
	}
	parsed, _, err := readSparseMap(bytes.NewReader(plan.sparseMap), plan.logical)
	if err != nil || len(parsed) != len(plan.extents) {
		t.Fatal("writer/reader boundary differs", len(parsed), err)
	}
	for _, encrypted := range []bool{false, true} {
		s.size = uint64(maxSparseExtents) * 8192
		var output bytes.Buffer
		var options []WriteOption
		if encrypted {
			options = append(options, WithCodec(&optionTestCodec{}, true))
		}
		_, _, err := WriteTo(context.Background(), &output, "disk", s, options...)
		if err == nil || !strings.Contains(err.Error(), "extent limit") || output.Len() != 0 || s.reads != 0 {
			t.Fatal("over-limit writer emitted or read payload", encrypted, output.Len(), s.reads, err)
		}
	}
}

func TestWriterIndexBudgetOptions(t *testing.T) {
	for _, opts := range [][]WriteOption{{WithIndexMemoryLimit(0)}, {WithIndexMemoryLimit(4 << 20), WithIndexMemoryLimit(4 << 20)}} {
		var output bytes.Buffer
		if _, _, err := WriteTo(context.Background(), &output, "empty", &commitSource{}, opts...); err == nil {
			t.Fatal("invalid budget options accepted")
		}
		if output.Len() != 0 {
			t.Fatal("invalid options emitted output")
		}
	}
}
