// Package sparse defines the platform-wide model for sparse byte
// sources: a three-state classification (Hole / Zero / Data) over a
// fixed-size logical image, queried as metadata (RunAt) and read as
// data (ReadAt).
//
// The Source contract separates the two concerns by law:
//
//  1. RunAt is a pure metadata query — answerable for any offset at
//     any time, never consuming data. A source's sparse map is known
//     up front (filesystem allocation, manifest metadata, tar sparse
//     map); it is never derived by scanning content.
//  2. ReadAt's baseline guarantee is monotone consumption: calls at
//     non-decreasing, non-overlapping offsets. One-pass sources
//     (pipes, tar streams) are first-class Sources.
//  3. Consumers call ReadAt only inside Data runs; Hole and Zero runs
//     are synthesized as zeros by the consumer (ReadAt over them must
//     still work and yield zeros).
//  4. Random or concurrent access is not part of this contract.
//     Consumers that need it require a stronger interface
//     (manifest/fetch.Stream strengthens ReadAt and RunAt to
//     concurrent random access).
//
// Semantics of the three states:
//
//   - Hole: no content in this layer — authoritative absence metadata. In an
//     overlay it falls through to lower layers; it is restored as a
//     real hole and never conflated with zero-valued data.
//   - Zero: logically present content whose bytes are all zero. It masks
//     lower layers like Data, but consumers can synthesize its bytes without
//     payload I/O. Manifest chunks, DIFF presence metadata and page-profile
//     tarstreams can represent it without storing whole zero pages. Other
//     carriers may encode literal zero bytes while preserving presence.
//   - Data: bytes that must be read.
//
// Holes only ever come from authoritative metadata. Deriving holes
// from content (zero scanning) is forbidden platform-wide: written
// zeros and holes carry different business meanings.
package sparse

import (
	"context"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"io"
)

// Extent is a byte range [Offset, Offset+Size) of the logical image.
type Extent struct {
	Offset, Size uint64
}

// RunKind classifies the content of one run of the logical image.
type RunKind uint8

const (
	// Hole: no data — authoritative absence. Falls through in overlays.
	Hole RunKind = iota
	// Zero: present zero content; masks lower layers without payload I/O.
	Zero
	// Data: bytes that must be read.
	Data
)

// Run is one already-resolved forward region [Offset(), End()) of a Source.
// ReadAt offsets are relative to Offset(); innerOffset+len(buf) must remain
// within the Run and an out-of-range request returns an error rather than
// truncating or entering the next Run. A successful non-empty read returns
// len(buf), nil. A zero-length read at or before End returns 0, nil. Hole and
// Zero Runs clear buf without payload I/O.
//
// A Run is immutable and remains valid only for the lifetime of its Source.
// Its concurrency guarantee is inherited from that Source: the baseline is
// monotone, single-goroutine use, while manifest/fetch.Stream strengthens its
// Runs to concurrent random access.
type Run interface {
	Offset() uint64
	End() uint64
	Kind() RunKind

	ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error)
}

// Source is the platform's sparse byte source. limit in RunAt is a
// length: the returned run satisfies offset < end <= min(offset+limit,
// Size()). See the package comment for the contract laws.
type Source interface {
	// Size is the total logical image size, holes included.
	Size() uint64

	// RunAt resolves the forward run beginning at offset. err == io.EOF iff
	// offset >= Size(); limit must be non-zero; otherwise err == nil and the
	// result satisfies Offset() == offset and
	// offset < End() <= min(offset+limit, Size()). Run granularity is
	// implementation-defined (a Data run may end before any state change).
	// RunAt is a pure metadata operation and never reads payload data.
	RunAt(offset, limit uint64) (Run, error)

	// ReadAt reads len(buf) bytes from offset:
	//
	//	(len(buf), nil)  full read
	//	(n, io.EOF)      n < len(buf): clipped at Size(); buf[:n] valid
	//	(0, io.EOF)      offset >= Size()
	//	(0, err)         transport / source error
	//
	// Hole and Zero runs read as zeros. Baseline contract is monotone
	// single-goroutine use (law 2); implementations may strengthen it.
	ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error)
}

// NewSource builds a Source over a random-access image: ra serves the
// bytes of [0, size) (hole regions, if ever read, must yield zeros —
// an *os.File over a sparse file does), holes is the authoritative
// hole map. Holes may be unsorted; zero-size extents are dropped and
// adjacent extents merge. Overlapping or out-of-bounds holes are an
// error. The source never returns Zero runs, and is as concurrent-safe
// as ra.
func NewSource(ra io.ReaderAt, size uint64, holes []Extent) (Source, error) {
	normalized, err := normalizeHoles(size, holes)
	if err != nil {
		return nil, fmt.Errorf("sparse: %w", err)
	}
	return &staticSource{ra: ra, size: size, holes: normalized}, nil
}

type staticSource struct {
	ra    io.ReaderAt
	size  uint64
	holes []Extent // sorted, merged, disjoint, in bounds
}

func (s *staticSource) Size() uint64 { return s.size }

func (s *staticSource) RunAt(offset, limit uint64) (Run, error) {
	limEnd, err := boundedRunEnd(s.size, offset, limit)
	if err != nil {
		return nil, err
	}
	if h, in := holeAt(s.holes, offset); in {
		end := h.Offset + h.Size
		if end > limEnd {
			end = limEnd
		}
		return sourceRun[*staticSource]{source: s, offset: offset, end: end, kind: Hole}, nil
	}
	return sourceRun[*staticSource]{source: s, offset: offset, end: nextHoleStart(s.holes, offset, limEnd), kind: Data}, nil
}

func (s *staticSource) ReadAt(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= s.size {
		return 0, io.EOF
	}
	n := len(buf)
	var eof error
	if offset+uint64(n) > s.size {
		n = int(s.size - offset)
		eof = io.EOF
	}
	m, err := s.ra.ReadAt(buf[:n], int64(offset))
	if err == io.ErrUnexpectedEOF {
		err = readerr.Mark(err, false)
	}
	if err != nil && err != io.EOF {
		return m, fmt.Errorf("sparse: read @ %d: %w", offset, err)
	}
	if m < n {
		return m, readerr.Mark(fmt.Errorf("sparse: short read @ %d: %d of %d bytes", offset, m, n), false)
	}
	return n, eof
}

// Dense wraps a sequential reader of exactly size bytes as a one-pass
// Source: a single Data run, ReadAt at monotone offsets (forward gaps
// are skipped by discarding). A source that ends before size yields
// io.ErrUnexpectedEOF. Not safe for concurrent use.
func Dense(r io.Reader, size uint64) Source {
	return &denseSource{r: r, size: size}
}

type denseSource struct {
	r    io.Reader
	size uint64
	pos  uint64 // bytes consumed from r
}

func (s *denseSource) Size() uint64 { return s.size }

func (s *denseSource) RunAt(offset, limit uint64) (Run, error) {
	limEnd, err := boundedRunEnd(s.size, offset, limit)
	if err != nil {
		return nil, err
	}
	return sourceRun[*denseSource]{source: s, offset: offset, end: limEnd, kind: Data}, nil
}

func (s *denseSource) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset >= s.size {
		return 0, io.EOF
	}
	if offset < s.pos {
		return 0, readerr.Mark(fmt.Errorf("sparse: dense source: backward read @ %d (consumed through %d)", offset, s.pos), false)
	}
	if offset > s.pos {
		if _, err := io.CopyN(io.Discard, s.r, int64(offset-s.pos)); err != nil {
			return 0, s.short(err)
		}
		s.pos = offset
	}
	n := len(buf)
	var eof error
	if offset+uint64(n) > s.size {
		n = int(s.size - offset)
		eof = io.EOF
	}
	// io.ReadFull suppresses any error accompanying a full buffer, including
	// an explicit source failure. Observe that cause before accepting bytes.
	reader := &observedReader{Reader: s.r}
	consumed, err := io.ReadFull(reader, buf[:n])
	s.pos += uint64(consumed)
	if err == nil && reader.err != io.EOF {
		err = reader.err
	}
	if err != nil {
		return 0, s.short(err)
	}
	return n, eof
}

type observedReader struct {
	io.Reader
	err error
}

func (r *observedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.err = err
	return n, err
}

// short maps an early end of the underlying reader to a hard error:
// the caller declared size bytes, so a shorter stream is corruption,
// not EOF.
func (s *denseSource) short(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return readerr.Mark(fmt.Errorf("sparse: dense source ended early (declared %d bytes): %w", s.size, io.ErrUnexpectedEOF), false)
	}
	return err
}

var errInvalidRun = readerr.Mark(errors.New("sparse: invalid run"), false)

// sourceRun is the lightweight Run used by the built-in Sources. Data reads
// delegate to Source.ReadAt so one-pass and concurrency guarantees are
// inherited unchanged; Hole and Zero reads never touch the payload source.
type runReader interface {
	ReadAt(context.Context, []byte, uint64) (int, error)
}

type sourceRun[S runReader] struct {
	source S
	offset uint64
	end    uint64
	kind   RunKind
}

func (r sourceRun[S]) Offset() uint64 { return r.offset }
func (r sourceRun[S]) End() uint64    { return r.end }
func (r sourceRun[S]) Kind() RunKind  { return r.kind }

func (r sourceRun[S]) ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error) {
	if err := validateRunRead(r.offset, r.end, innerOffset, len(buf)); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.kind == Hole || r.kind == Zero {
		clear(buf)
		return len(buf), nil
	}

	n, err := r.source.ReadAt(ctx, buf, r.offset+innerOffset)
	if n == len(buf) && (err == nil || err == io.EOF) {
		return n, nil
	}
	if err != nil {
		return n, err
	}
	return n, readerr.Mark(fmt.Errorf("sparse: short run read @ %d: %d of %d bytes", r.offset+innerOffset, n, len(buf)), false)
}

func boundedRunEnd(size, offset, limit uint64) (uint64, error) {
	if offset >= size {
		return 0, io.EOF
	}
	if limit == 0 {
		return 0, fmt.Errorf("%w: zero limit at offset %d", errInvalidRun, offset)
	}
	if limit >= size-offset {
		return size, nil
	}
	return offset + limit, nil
}

func validateRunRead(offset, end, innerOffset uint64, length int) error {
	if end <= offset {
		return fmt.Errorf("%w: run [%d,%d)", errInvalidRun, offset, end)
	}
	runLength := end - offset
	if innerOffset > runLength || uint64(length) > runLength-innerOffset {
		return fmt.Errorf("%w: read offset %d length %d outside [0,%d)", errInvalidRun, innerOffset, length, runLength)
	}
	return nil
}

// normalizeHoles validates and canonicalizes a hole map: copy, sort,
// drop zero-size extents, merge adjacent ones; overlap and
// out-of-bounds are errors.
func normalizeHoles(size uint64, holes []Extent) ([]Extent, error) {
	hs := make([]Extent, 0, len(holes))
	for _, h := range holes {
		if h.Size == 0 {
			continue
		}
		end := h.Offset + h.Size
		if end < h.Offset || end > size {
			return nil, fmt.Errorf("hole [%d, %d) out of bounds (size %d)", h.Offset, end, size)
		}
		hs = append(hs, h)
	}
	if len(hs) == 0 {
		return nil, nil
	}
	sortExtents(hs)
	merged := hs[:1]
	for _, h := range hs[1:] {
		last := &merged[len(merged)-1]
		switch {
		case h.Offset < last.Offset+last.Size:
			return nil, errors.New("overlapping holes")
		case h.Offset == last.Offset+last.Size: // adjacent: merge
			last.Size += h.Size
		default:
			merged = append(merged, h)
		}
	}
	return merged, nil
}

// sortExtents — insertion sort: extent maps are small and usually
// already sorted.
func sortExtents(s []Extent) {
	for i := 1; i < len(s); i++ {
		x := s[i]
		j := i - 1
		for j >= 0 && s[j].Offset > x.Offset {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = x
	}
}

// holeAt returns the hole extent containing offset, or ok=false. holes
// is sorted and disjoint. O(log n).
func holeAt(holes []Extent, offset uint64) (Extent, bool) {
	lo, hi := 0, len(holes)-1
	idx := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if holes[mid].Offset <= offset {
			idx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if idx < 0 {
		return Extent{}, false
	}
	if h := holes[idx]; offset < h.Offset+h.Size {
		return h, true
	}
	return Extent{}, false
}

// nextHoleStart returns the start of the first hole at or after offset
// (the caller must be in data at offset), clamped to limEnd — used to
// bound a data run.
func nextHoleStart(holes []Extent, offset, limEnd uint64) uint64 {
	lo, hi := 0, len(holes)
	for lo < hi {
		mid := (lo + hi) / 2
		if holes[mid].Offset < offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(holes) && holes[lo].Offset < limEnd {
		return holes[lo].Offset
	}
	return limEnd
}
