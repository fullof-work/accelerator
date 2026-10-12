package tarstream

import (
	"bytes"
	"context"
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
