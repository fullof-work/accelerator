//go:build linux

package flatten

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"unsafe"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

const (
	planHeaderSize = 192
	planRecordSize = 64
	planBlockSize  = 4096
	planReadBatch  = 1 << 20
)

// identity deliberately excludes atime: reading the source may update it.
type fileIdentity struct {
	dev, ino, size, nlink            uint64
	mtime, ctime                     int64
	mtimeNS, ctimeNS, mode, uid, gid uint32
}

func fileID(f *os.File) (fileIdentity, error) {
	return descriptorID(int(f.Fd()))
}

func descriptorID(fd int) (fileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fileIdentity{}, err
	}
	return statID(&st), nil
}

func statID(st *unix.Stat_t) fileIdentity {
	return fileIdentity{dev: uint64(st.Dev), ino: st.Ino, size: uint64(st.Size), nlink: uint64(st.Nlink),
		mtime: st.Mtim.Sec, ctime: st.Ctim.Sec, mtimeNS: uint32(st.Mtim.Nsec), ctimeNS: uint32(st.Ctim.Nsec),
		mode: st.Mode, uid: st.Uid, gid: st.Gid}
}

type planFile struct {
	name string
	id   fileIdentity
}

// The source table must fit an addressable Go slice. Do not add a separate
// arbitrary file-count limit below the byte and representation checks.
const maxPlanSources = uint64(math.MaxInt) / uint64(unsafe.Sizeof(planFile{}))

type chunkRecord struct {
	source, offset uint64
	length         uint32
	digest         [32]byte
}

// erofsPlan owns all of its descriptors. Its Source is single-consumer; RunAt
// is repeatable metadata and never reads a source file or advances a cursor.
type erofsPlan struct {
	header                             [planHeaderSize]byte
	layout, records, sourceIndex       *os.File
	root                               *os.Root
	rootDescriptor                     *os.File
	layoutID, recordsID, sourceIndexID fileIdentity
	size, dataStart, dataLength, count uint64
	files                              []planFile
	holes                              []sparse.Extent
	indexBytes                         uint64
	current                            *planSource
	currentID                          uint64
	data                               []byte
	dataStartOffset                    uint64
	dataValid                          int
	recordPage                         [64 << 10]byte
	recordPageStart                    uint64
	recordPageValid                    int
}

func openPlan(ctx context.Context, root, layout, name string, limits PlanLimits) (_ *erofsPlan, retErr error) {
	limits, err := limits.defaults()
	if err != nil {
		return nil, err
	}
	p := &erofsPlan{}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, p.Close())
		}
	}()
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	_, err = io.ReadFull(f, p.header[:])
	st, statErr := f.Stat()
	err = errors.Join(err, statErr, f.Close())
	if err != nil {
		return nil, fmt.Errorf("flatten: read plan: %w", err)
	}
	if st.Size() != planHeaderSize || string(p.header[:8]) != "KERPLAN\x00" ||
		binary.LittleEndian.Uint32(p.header[8:]) != 1 || binary.LittleEndian.Uint32(p.header[12:]) != planHeaderSize ||
		binary.LittleEndian.Uint64(p.header[16:]) != 1 || !bytes.Equal(p.header[176:], make([]byte, 16)) {
		return nil, fmt.Errorf("flatten: unsupported or malformed plan header")
	}
	u64 := func(off int) uint64 { return binary.LittleEndian.Uint64(p.header[off:]) }
	p.size, p.dataStart, p.dataLength, p.count = u64(24), u64(32), u64(40), u64(48)
	fileCount, recordBytes, sourceBytes := u64(56), u64(64), u64(72)
	if p.size == 0 || p.size > math.MaxInt64 || p.size%planBlockSize != 0 ||
		p.dataStart > p.size || p.dataStart%planBlockSize != 0 || p.dataLength > p.size-p.dataStart ||
		p.count > math.MaxInt64/planBlockSize || p.dataLength != p.count*planBlockSize ||
		recordBytes != p.count*planRecordSize || fileCount > maxPlanSources || fileCount > sourceBytes/81 ||
		sourceBytes > limits.PlanBytes || recordBytes > limits.PlanBytes-sourceBytes ||
		planHeaderSize > limits.PlanBytes-sourceBytes-recordBytes ||
		p.size-p.dataLength > limits.PlanBytes-sourceBytes-recordBytes-planHeaderSize {
		return nil, fmt.Errorf("flatten: plan bounds or byte budget exceeded")
	}
	// Charge the source structs, path storage, read buffers, and spare space for
	// allocation rounding before allocating. No per-chunk Go index is created.
	if sourceBytes > limits.IndexMemoryBytes/2 || fileCount > limits.IndexMemoryBytes/256 {
		return nil, fmt.Errorf("flatten: plan index memory budget exceeded")
	}
	p.indexBytes = 2*sourceBytes + fileCount*256 + 2*planReadBatch
	if p.indexBytes > limits.IndexMemoryBytes {
		return nil, fmt.Errorf("flatten: plan index memory budget exceeded")
	}
	if p.layout, err = os.Open(layout); err != nil {
		return nil, err
	}
	if p.records, err = os.Open(name + ".chunks"); err != nil {
		return nil, err
	}
	if p.sourceIndex, err = os.Open(name + ".sources"); err != nil {
		return nil, err
	}
	if p.root, err = os.OpenRoot(root); err != nil {
		return nil, err
	}
	if p.rootDescriptor, err = p.root.OpenFile(".", unix.O_PATH|unix.O_DIRECTORY, 0); err != nil {
		return nil, err
	}
	for _, item := range []struct {
		f    *os.File
		id   *fileIdentity
		size uint64
	}{
		{p.layout, &p.layoutID, p.size}, {p.records, &p.recordsID, recordBytes}, {p.sourceIndex, &p.sourceIndexID, sourceBytes},
	} {
		*item.id, err = fileID(item.f)
		if err != nil {
			return nil, err
		}
		if item.id.mode&unix.S_IFMT != unix.S_IFREG || item.id.size != item.size {
			return nil, fmt.Errorf("flatten: plan file size/type mismatch")
		}
	}
	if err = p.verifyPlanFiles(ctx); err != nil {
		return nil, err
	}
	p.files = make([]planFile, int(fileCount))
	r := bufio.NewReaderSize(io.NewSectionReader(p.sourceIndex, 0, int64(sourceBytes)), 64<<10)
	for i := range p.files {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var b [80]byte
		if _, err = io.ReadFull(r, b[:]); err != nil {
			return nil, fmt.Errorf("flatten: truncated plan source: %w", err)
		}
		get64 := func(off int) uint64 { return binary.LittleEndian.Uint64(b[off:]) }
		get32 := func(off int) uint32 { return binary.LittleEndian.Uint32(b[off:]) }
		n := get32(68)
		if get64(0) != uint64(i+1) || n == 0 || n > 4096 {
			return nil, fmt.Errorf("flatten: invalid plan source entry")
		}
		name := make([]byte, int(n))
		if _, err = io.ReadFull(r, name); err != nil {
			return nil, err
		}
		f := &p.files[i]
		f.name = string(name)
		f.id = fileIdentity{dev: get64(8), ino: get64(16), size: get64(24), nlink: get64(72),
			mtime: int64(get64(32)), ctime: int64(get64(40)), mtimeNS: get32(48), ctimeNS: get32(52), mode: get32(56), uid: get32(60), gid: get32(64)}
		if !validPlanPath(f.name) || f.id.size > math.MaxInt64 || f.id.mode&unix.S_IFMT != unix.S_IFREG ||
			f.id.mtimeNS >= 1000000000 || f.id.ctimeNS >= 1000000000 || f.id.nlink == 0 {
			return nil, fmt.Errorf("flatten: invalid plan source identity")
		}
	}
	if _, err = r.ReadByte(); err != io.EOF {
		return nil, fmt.Errorf("flatten: excess plan source bytes")
	}
	var previousID uint64
	for i := uint64(0); i < p.count; i++ {
		if i%1024 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
		}
		rec, e := p.record(i)
		if e != nil {
			return nil, e
		}
		if rec.source < previousID {
			return nil, fmt.Errorf("flatten: unordered plan sources")
		}
		previousID = rec.source
	}
	// The payload is intentionally absent from layout. Remove this entire
	// interval from allocation holes so that every chunk is emitted as Data.
	p.holes, err = planHoles(p.layout, p.size, p.dataStart, p.dataLength, (limits.IndexMemoryBytes-p.indexBytes)/64)
	if err != nil {
		return nil, err
	}
	p.indexBytes += uint64(cap(p.holes)) * 16
	p.data = make([]byte, planReadBatch)
	return p, nil
}

func validPlanPath(name string) bool {
	return name != "." && name != ".." && !strings.HasPrefix(name, "../") &&
		!strings.HasPrefix(name, "/") && !strings.ContainsRune(name, 0) && path.Clean(name) == name
}

func (p *erofsPlan) record(i uint64) (chunkRecord, error) {
	var rec chunkRecord
	if i >= p.count {
		return rec, fmt.Errorf("flatten: plan record out of bounds")
	}
	off := i * planRecordSize
	if p.recordPageValid == 0 || off < p.recordPageStart || off+planRecordSize > p.recordPageStart+uint64(p.recordPageValid) {
		p.recordPageStart = off / uint64(len(p.recordPage)) * uint64(len(p.recordPage))
		want := min(uint64(len(p.recordPage)), p.count*planRecordSize-p.recordPageStart)
		n, err := p.records.ReadAt(p.recordPage[:want], int64(p.recordPageStart))
		if err != nil || uint64(n) != want {
			return rec, fmt.Errorf("flatten: read plan records: %w", errors.Join(err, io.ErrUnexpectedEOF))
		}
		p.recordPageValid = n
	}
	b := p.recordPage[off-p.recordPageStart : off-p.recordPageStart+planRecordSize]
	rec.source, rec.offset, rec.length = binary.LittleEndian.Uint64(b[16:]), binary.LittleEndian.Uint64(b[8:]), binary.LittleEndian.Uint32(b[24:])
	if binary.LittleEndian.Uint64(b) != off/planRecordSize*planBlockSize || rec.source == 0 || rec.source > uint64(len(p.files)) ||
		rec.length == 0 || rec.length > planBlockSize || binary.LittleEndian.Uint32(b[28:]) != planBlockSize-rec.length || rec.offset%planBlockSize != 0 {
		return rec, fmt.Errorf("flatten: invalid plan chunk record")
	}
	f := p.files[rec.source-1]
	if rec.offset >= f.id.size || uint64(rec.length) != min(uint64(planBlockSize), f.id.size-rec.offset) {
		return rec, fmt.Errorf("flatten: plan chunk exceeds source")
	}
	copy(rec.digest[:], b[32:])
	return rec, nil
}

func (p *erofsPlan) verifyPlanFiles(ctx context.Context) error {
	for _, item := range []struct {
		f                     *os.File
		id                    fileIdentity
		hash                  []byte
		skipStart, skipLength uint64
	}{
		{p.layout, p.layoutID, p.header[80:112], p.dataStart, p.dataLength},
		{p.records, p.recordsID, p.header[112:144], 0, 0},
		{p.sourceIndex, p.sourceIndexID, p.header[144:176], 0, 0},
	} {
		id, err := fileID(item.f)
		if err != nil {
			return err
		}
		if id != item.id {
			return fmt.Errorf("flatten: plan file changed")
		}
		h := sha256.New()
		var buf [64 << 10]byte
		for _, interval := range [][2]uint64{{0, item.skipStart}, {item.skipStart + item.skipLength, id.size}} {
			for off := interval[0]; off < interval[1]; {
				if err := ctx.Err(); err != nil {
					return err
				}
				want := min(uint64(len(buf)), interval[1]-off)
				n, err := item.f.ReadAt(buf[:want], int64(off))
				if err != nil || uint64(n) != want {
					return fmt.Errorf("flatten: truncated plan file: %w", errors.Join(err, io.ErrUnexpectedEOF))
				}
				h.Write(buf[:n])
				off += uint64(n)
			}
		}
		if !bytes.Equal(h.Sum(nil), item.hash) {
			return fmt.Errorf("flatten: plan digest mismatch")
		}
		id, err = fileID(item.f)
		if err != nil {
			return err
		}
		if id != item.id {
			return fmt.Errorf("flatten: plan file changed during validation")
		}
	}
	return nil
}

func planHoles(f *os.File, size, start, length, maxHoles uint64) ([]sparse.Extent, error) {
	var holes []sparse.Extent
	appendHole := func(a, b uint64) error {
		if a == b {
			return nil
		}
		if uint64(len(holes)) >= maxHoles {
			return fmt.Errorf("flatten: layout extent budget exceeded")
		}
		holes = append(holes, sparse.Extent{Offset: a, Size: b - a})
		return nil
	}
	for cursor := uint64(0); cursor < size; {
		hole, err := unix.Seek(int(f.Fd()), int64(cursor), unix.SEEK_HOLE)
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if uint64(hole) >= size {
			break
		}
		data, err := unix.Seek(int(f.Fd()), hole, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			data, err = int64(size), nil
		}
		if err != nil {
			return nil, err
		}
		if hole < int64(cursor) || data <= hole || uint64(data) > size {
			return nil, fmt.Errorf("flatten: invalid layout allocation map")
		}
		a, b := uint64(hole), uint64(data)
		if a < start {
			if err = appendHole(a, min(b, start)); err != nil {
				return nil, err
			}
		}
		if b > start+length {
			if err = appendHole(max(a, start+length), b); err != nil {
				return nil, err
			}
		}
		cursor = b
	}
	return holes, nil
}

func (p *erofsPlan) Size() uint64 { return p.size }

func (p *erofsPlan) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= p.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("flatten: zero run limit")
	}
	end, kind := off+min(limit, p.size-off), sparse.Data
	// The bounded extent list is sorted by allocation order. Binary search is
	// pure metadata; callers can query backward without disturbing emission.
	lo, hi := 0, len(p.holes)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if p.holes[m].Offset+p.holes[m].Size <= off {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(p.holes) {
		h := p.holes[lo]
		if h.Offset <= off {
			kind, end = sparse.Hole, min(end, h.Offset+h.Size)
		} else {
			end = min(end, h.Offset)
		}
	}
	return planRun{p, off, end, kind}, nil
}

type planRun struct {
	p           *erofsPlan
	offset, end uint64
	kind        sparse.RunKind
}

func (r planRun) Offset() uint64       { return r.offset }
func (r planRun) End() uint64          { return r.end }
func (r planRun) Kind() sparse.RunKind { return r.kind }
func (r planRun) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if off > r.end-r.offset || uint64(len(b)) > r.end-r.offset-off {
		return 0, fmt.Errorf("flatten: read outside plan run")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	if r.kind == sparse.Hole {
		clear(b)
		return len(b), nil
	}
	return r.p.ReadAt(ctx, b, r.offset+off)
}

func (p *erofsPlan) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off >= p.size {
		return 0, io.EOF
	}
	want := min(uint64(len(b)), p.size-off)
	n := 0
	for uint64(n) < want {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		pos := off + uint64(n)
		if pos < p.dataStart || pos >= p.dataStart+p.dataLength {
			end := p.size
			if pos < p.dataStart {
				end = p.dataStart
			}
			length := min(want-uint64(n), end-pos)
			got, err := p.layout.ReadAt(b[n:n+int(length)], int64(pos))
			n += got
			if err != nil || uint64(got) != length {
				return n, fmt.Errorf("flatten: read layout: %w", errors.Join(err, io.ErrUnexpectedEOF))
			}
			continue
		}
		rel := pos - p.dataStart
		rec, err := p.record(rel / planBlockSize)
		if err != nil {
			return n, err
		}
		data, err := p.chunk(ctx, rec)
		if err != nil {
			return n, err
		}
		inner := rel % planBlockSize
		length := min(want-uint64(n), planBlockSize-inner)
		clear(b[n : n+int(length)])
		if inner < uint64(len(data)) {
			copy(b[n:n+int(length)], data[inner:])
		}
		n += int(length)
	}
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *erofsPlan) chunk(ctx context.Context, rec chunkRecord) ([]byte, error) {
	f := p.files[rec.source-1]
	if p.currentID != rec.source {
		if p.current != nil {
			if err := p.current.Close(); err != nil {
				return nil, err
			}
			p.current = nil
		}
		p.currentID, p.dataValid = 0, 0
		var err error
		p.current, err = openPlanSource(p.root, p.rootDescriptor, f.name)
		if err != nil {
			return nil, fmt.Errorf("flatten: open planned source %q: %w", f.name, err)
		}
		p.currentID = rec.source
	}
	if p.dataValid == 0 || rec.offset < p.dataStartOffset || rec.offset+uint64(rec.length) > p.dataStartOffset+uint64(p.dataValid) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id, err := descriptorID(p.current.fd)
		if err != nil {
			return nil, err
		}
		if id != f.id {
			return nil, fmt.Errorf("flatten: source %q changed", f.name)
		}
		p.dataStartOffset = rec.offset / planReadBatch * planReadBatch
		want := min(uint64(len(p.data)), f.id.size-p.dataStartOffset)
		n, err := p.current.readAt(ctx, p.data[:want], int64(p.dataStartOffset))
		if err != nil || uint64(n) != want {
			return nil, fmt.Errorf("flatten: truncated source %q: %w", f.name, errors.Join(err, io.ErrUnexpectedEOF))
		}
		p.dataValid = n
	}
	start := rec.offset - p.dataStartOffset
	data := p.data[start : start+uint64(rec.length)]
	if sha256.Sum256(data) != rec.digest {
		return nil, fmt.Errorf("flatten: source chunk digest mismatch for %q", f.name)
	}
	return data, nil
}

func (p *erofsPlan) Close() error {
	var errs []error
	if p.current != nil {
		errs = append(errs, p.current.Close())
		p.current = nil
	}
	for _, f := range []*os.File{p.rootDescriptor, p.layout, p.records, p.sourceIndex} {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	p.rootDescriptor, p.layout, p.records, p.sourceIndex = nil, nil, nil, nil
	if p.root != nil {
		errs = append(errs, p.root.Close())
		p.root = nil
	}
	return errors.Join(errs...)
}
