package sparse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// image builds a dense byte image of size n with data bytes 'A' in the
// given extents and zeros elsewhere (what an *os.File over a sparse
// file would read).
func image(n uint64, data ...Extent) []byte {
	buf := make([]byte, n)
	for _, e := range data {
		for i := e.Offset; i < e.Offset+e.Size; i++ {
			buf[i] = 'A'
		}
	}
	return buf
}

func TestNewSourceNormalization(t *testing.T) {
	const size = 1 << 20
	ra := bytes.NewReader(image(size))

	// Unsorted + adjacent holes merge; zero-size dropped.
	src, err := NewSource(ra, size, []Extent{
		{Offset: 8192, Size: 4096},
		{Offset: 4096, Size: 4096},
		{Offset: 100, Size: 0},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	run, err := src.RunAt(4096, size)
	if err != nil || run.Kind() != Hole || run.Offset() != 4096 || run.End() != 12288 {
		t.Fatalf("merged hole run = (%v, %d, %d, %v), want (Hole, 4096, 12288, nil)", run.Kind(), run.Offset(), run.End(), err)
	}

	if _, err := NewSource(ra, size, []Extent{{0, 4096}, {2048, 4096}}); err == nil {
		t.Fatal("overlapping holes accepted")
	}
	if _, err := NewSource(ra, size, []Extent{{size - 1024, 2048}}); err == nil {
		t.Fatal("out-of-bounds hole accepted")
	}
}

func TestStaticSourceRuns(t *testing.T) {
	// [0,8K) data, [8K,1M) hole, [1M,1M+4K) data, [1M+4K,3M) hole
	const size = 3 << 20
	holes := []Extent{{8192, (1 << 20) - 8192}, {(1 << 20) + 4096, (2 << 20) - 4096}}
	src, err := NewSource(bytes.NewReader(image(size, Extent{0, 8192}, Extent{1 << 20, 4096})), size, holes)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}

	type run struct {
		kind RunKind
		end  uint64
	}
	var got []run
	for off := uint64(0); ; {
		resolved, err := src.RunAt(off, size)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("RunAt(%d): %v", off, err)
		}
		got = append(got, run{resolved.Kind(), resolved.End()})
		off = resolved.End()
	}
	want := []run{
		{Data, 8192},
		{Hole, 1 << 20},
		{Data, (1 << 20) + 4096},
		{Hole, 3 << 20},
	}
	if len(got) != len(want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("run[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// limit clamps the run end.
	if run, _ := src.RunAt(8192, 100); run.End() != 8292 {
		t.Fatalf("limited hole run end = %d, want 8292", run.End())
	}

	// ReadAt spans data, clips at EOF.
	buf := make([]byte, 8192)
	if n, err := src.ReadAt(context.Background(), buf, 0); n != 8192 || err != nil {
		t.Fatalf("ReadAt data = (%d, %v)", n, err)
	}
	if buf[0] != 'A' || buf[8191] != 'A' {
		t.Fatal("data bytes wrong")
	}
	if n, err := src.ReadAt(context.Background(), buf, size-100); n != 100 || err != io.EOF {
		t.Fatalf("clipped ReadAt = (%d, %v), want (100, EOF)", n, err)
	}
	if n, err := src.ReadAt(context.Background(), buf, size); n != 0 || err != io.EOF {
		t.Fatalf("past-EOF ReadAt = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestDenseSource(t *testing.T) {
	payload := []byte(strings.Repeat("x", 1000))
	src := Dense(bytes.NewReader(payload), 1000)

	if run, err := src.RunAt(0, 1<<20); err != nil || run.Kind() != Data || run.Offset() != 0 || run.End() != 1000 {
		t.Fatalf("RunAt = (%v, %d, %d, %v), want (Data, 0, 1000, nil)", run.Kind(), run.Offset(), run.End(), err)
	}
	if run, err := src.RunAt(10, 20); err != nil || run.Kind() != Data || run.Offset() != 10 || run.End() != 30 {
		t.Fatalf("limited RunAt = (%v, %d, %d, %v)", run.Kind(), run.Offset(), run.End(), err)
	}
	if _, err := src.RunAt(0, 0); err == nil {
		t.Fatal("dense RunAt accepted zero limit")
	}
	if _, err := src.RunAt(1000, 1); !errors.Is(err, io.EOF) {
		t.Fatalf("dense RunAt at EOF = %v, want io.EOF", err)
	}

	ctx := context.Background()
	buf := make([]byte, 100)
	if n, err := src.ReadAt(ctx, buf, 0); n != 100 || err != nil {
		t.Fatalf("first read = (%d, %v)", n, err)
	}
	// Forward gap: skipped by discarding.
	if n, err := src.ReadAt(ctx, buf, 300); n != 100 || err != nil {
		t.Fatalf("forward read = (%d, %v)", n, err)
	}
	// Backward read: error.
	if _, err := src.ReadAt(ctx, buf, 100); err == nil {
		t.Fatal("backward read accepted")
	}
	// Clip at size.
	if n, err := src.ReadAt(ctx, buf, 950); n != 50 || err != io.EOF {
		t.Fatalf("clipped read = (%d, %v), want (50, EOF)", n, err)
	}
}

func TestDenseSourceShortInput(t *testing.T) {
	src := Dense(bytes.NewReader([]byte("short")), 100)
	buf := make([]byte, 100)
	_, err := src.ReadAt(context.Background(), buf, 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short input error = %v, want ErrUnexpectedEOF", err)
	}
}

func TestProbeHoles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// [0,8K) data, [8K,1M) hole, [1M,1M+4K) data, [1M+4K,3M) hole
	if _, err := f.WriteAt(bytes.Repeat([]byte{'A'}, 8192), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{'B'}, 4096), 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 << 20); err != nil {
		t.Fatal(err)
	}

	holes, err := ProbeHoles(f)
	if err != nil {
		t.Fatalf("ProbeHoles: %v", err)
	}
	if len(holes) == 0 {
		t.Skip("filesystem does not expose holes (SEEK_HOLE returned dense)")
	}
	// Filesystems may round hole boundaries to block size; verify the
	// declared holes are truly inside the zero regions and cover the
	// bulk of them.
	var holeBytes uint64
	for _, h := range holes {
		if h.Offset < 8192 && h.Offset+h.Size > 8192 {
			t.Fatalf("hole %+v overlaps data [0,8192)", h)
		}
		if h.Offset < (1<<20)+4096 && h.Offset+h.Size > 1<<20 {
			t.Fatalf("hole %+v overlaps data [1M,1M+4K)", h)
		}
		holeBytes += h.Size
	}
	if want := uint64(3<<20 - 8192 - 4096); holeBytes < want/2 {
		t.Fatalf("holes cover %d bytes, want at least %d", holeBytes, want/2)
	}
	// Offset restored.
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != 0 {
		t.Fatalf("file offset = %d after probe, want 0", pos)
	}

	// Dense file probes to nil.
	dense := filepath.Join(dir, "dense.bin")
	if err := os.WriteFile(dense, bytes.Repeat([]byte{'C'}, 65536), 0o644); err != nil {
		t.Fatal(err)
	}
	df, err := os.Open(dense)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	holes, err = ProbeHoles(df)
	if err != nil {
		t.Fatalf("ProbeHoles(dense): %v", err)
	}
	if len(holes) != 0 {
		t.Fatalf("dense file probed holes: %+v", holes)
	}
}

func TestSourceContractZeroNeverFromStatic(t *testing.T) {
	// Static and dense sources must never classify anything as Zero —
	// Static sources know only byte-level holes and do not scan Data for zeros.
	const size = 1 << 16
	src, err := NewSource(bytes.NewReader(image(size)), size, []Extent{{0, 4096}})
	if err != nil {
		t.Fatal(err)
	}
	for off := uint64(0); ; {
		run, err := src.RunAt(off, size)
		if err == io.EOF {
			break
		}
		if run.Kind() == Zero {
			t.Fatalf("static source returned Zero at %d", off)
		}
		off = run.End()
	}
}

type countingReaderAt struct {
	data  []byte
	calls int
}

func (r *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	return bytes.NewReader(r.data).ReadAt(p, off)
}

func TestRunReadBoundsAndMetadataPurity(t *testing.T) {
	const size = uint64(32)
	ra := &countingReaderAt{data: bytes.Repeat([]byte{0xA5}, int(size))}
	src, err := NewSource(ra, size, []Extent{{Offset: 8, Size: 8}})
	if err != nil {
		t.Fatal(err)
	}

	dataRun, err := src.RunAt(2, 20)
	if err != nil {
		t.Fatal(err)
	}
	if ra.calls != 0 {
		t.Fatalf("RunAt performed %d payload reads", ra.calls)
	}
	if dataRun.Offset() != 2 || dataRun.End() != 8 || dataRun.Kind() != Data {
		t.Fatalf("data run = (%d,%d,%v), want (2,8,Data)", dataRun.Offset(), dataRun.End(), dataRun.Kind())
	}
	buf := make([]byte, 3)
	if n, err := dataRun.ReadAt(context.Background(), buf, 2); n != len(buf) || err != nil {
		t.Fatalf("in-run read = (%d,%v)", n, err)
	}
	if !bytes.Equal(buf, bytes.Repeat([]byte{0xA5}, len(buf))) || ra.calls != 1 {
		t.Fatalf("data read = %x, payload calls = %d", buf, ra.calls)
	}

	for _, tc := range []struct {
		name  string
		inner uint64
		len   int
	}{
		{name: "cross end", inner: 5, len: 2},
		{name: "past end empty", inner: 7, len: 0},
		{name: "overflow", inner: ^uint64(0), len: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := ra.calls
			if _, err := dataRun.ReadAt(context.Background(), make([]byte, tc.len), tc.inner); err == nil {
				t.Fatal("out-of-run read succeeded")
			}
			if ra.calls != before {
				t.Fatal("invalid read touched payload")
			}
		})
	}
	if n, err := dataRun.ReadAt(context.Background(), nil, dataRun.End()-dataRun.Offset()); n != 0 || err != nil {
		t.Fatalf("empty read at end = (%d,%v)", n, err)
	}

	holeRun, err := src.RunAt(8, 8)
	if err != nil {
		t.Fatal(err)
	}
	holeBuf := bytes.Repeat([]byte{0xFF}, 8)
	before := ra.calls
	if n, err := holeRun.ReadAt(context.Background(), holeBuf, 0); n != len(holeBuf) || err != nil {
		t.Fatalf("hole read = (%d,%v)", n, err)
	}
	if !bytes.Equal(holeBuf, make([]byte, len(holeBuf))) || ra.calls != before {
		t.Fatalf("hole read = %x, payload calls = %d", holeBuf, ra.calls)
	}
}

func TestRunAtErrorsAndOverflow(t *testing.T) {
	src, err := NewSource(bytes.NewReader(make([]byte, 16)), 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.RunAt(0, 0); err == nil {
		t.Fatal("zero RunAt limit succeeded")
	}
	if _, err := src.RunAt(16, 1); !errors.Is(err, io.EOF) {
		t.Fatalf("RunAt at EOF = %v, want io.EOF", err)
	}
	run, err := src.RunAt(4, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if run.Offset() != 4 || run.End() != 16 {
		t.Fatalf("overflow-clamped run = [%d,%d), want [4,16)", run.Offset(), run.End())
	}
}

func TestDenseRunReadKeepsOnePassSemantics(t *testing.T) {
	src := Dense(bytes.NewReader([]byte("0123456789abcdef")), 16)
	late, err := src.RunAt(8, 4)
	if err != nil {
		t.Fatal(err)
	}
	// Metadata lookup alone must not advance the reader.
	early, err := src.RunAt(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := early.ReadAt(context.Background(), buf, 0); err != nil || string(buf) != "0123" {
		t.Fatalf("early run read = %q, %v", buf, err)
	}
	if _, err := late.ReadAt(context.Background(), buf, 0); err != nil || string(buf) != "89ab" {
		t.Fatalf("late run read = %q, %v", buf, err)
	}
	if _, err := early.ReadAt(context.Background(), buf, 0); err == nil {
		t.Fatal("backward Run.ReadAt succeeded")
	}
}

func BenchmarkStaticRunAt(b *testing.B) {
	src, err := NewSource(bytes.NewReader(make([]byte, 1<<20)), 1<<20, []Extent{{Offset: 512 << 10, Size: 64 << 10}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		run, err := src.RunAt(uint64(i)&((1<<20)-1), 4096)
		if err != nil || run.End() <= run.Offset() {
			b.Fatal(run, err)
		}
	}
}
