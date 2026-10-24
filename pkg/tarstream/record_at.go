package tarstream

import (
	"fmt"
	"io"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
)

const encryptedDataOffset = envelopePrefixSize + envelopeHeaderSealed

const (
	recordReadBufferMinShift      = 12
	recordReadBufferMaxPowerShift = 20
	recordReadBufferLargePool     = recordReadBufferMaxPowerShift - recordReadBufferMinShift + 1
	recordReadBufferPoolCount     = recordReadBufferLargePool + 1
)

// The pools retain only scratch capacity. releaseRecordReadBuffer clears every
// byte used for ciphertext, plaintext, or AAD before making a buffer reusable.
var recordReadBufferPools [recordReadBufferPoolCount]sync.Pool

type recordReadBuffer struct {
	data      []byte
	poolIndex int
}

// recordReaderAt exposes the authenticated envelope as a plaintext io.ReaderAt.
// Record addressing is O(1). Reads coalesce physical I/O, authenticate every
// selected record, copy its requested plaintext directly to the caller, and do
// not retain plaintext. Cache ownership belongs to consumers and lower layers.
type recordReaderAt struct {
	ra       io.ReaderAt
	codec    RecordCodec
	prefix   [envelopePrefixSize]byte
	header   [envelopeHeaderSize]byte
	geometry envelopeHeader

	closed atomic.Bool
}

// recordMetadataReader is an opening-only scratch window, never retained by a
// source. It authenticates each cached record before exposing any metadata and
// does not read ahead across the authenticated packed-payload boundary.
type recordMetadataReader struct {
	records *recordReaderAt
	buffer  [recordSize]byte
	index   uint64
}

func (r *recordMetadataReader) ReadAt(dst []byte, off int64) (int, error) {
	g := r.records.geometry
	if off < 0 || len(dst) == 0 || uint64(off) >= g.plaintextSize {
		return r.records.ReadAt(dst, off)
	}
	pos := uint64(off)
	index := g.recordForOffset(pos)
	start, size := g.recordPlainStart(index), g.recordPlainSize(index)
	metadata := pos < g.packedStart || pos >= g.packedStart+g.packedSize
	if !metadata || uint64(len(dst)) > start+size-pos {
		return r.records.ReadAt(dst, off)
	}
	if r.index != index {
		r.index = ^uint64(0)
		clear(r.buffer[:])
		if err := readAtFull(r.records, r.buffer[:size], int64(start)); err != nil {
			return 0, err
		}
		r.index = index
	}
	return copy(dst, r.buffer[pos-start:pos-start+uint64(len(dst))]), nil
}

func openRecordReaderAt(ra io.ReaderAt, artifactSize int64, codec Codec) (*recordReaderAt, error) {
	if codec == nil {
		return nil, ErrCodecRequired
	}
	if artifactSize < encryptedDataOffset {
		return nil, fmt.Errorf("%w: truncated encrypted header", ErrMalformedEnvelope)
	}
	var prefix [envelopePrefixSize]byte
	if err := readAtFull(ra, prefix[:], 0); err != nil {
		return nil, &canonicalReadError{fmt.Errorf("%w: read clear prefix", ErrMalformedEnvelope), err}
	}
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	recordCodec, err := bindRecordCodec(codec, prefixSalt(prefix))
	if err != nil {
		return nil, err
	}
	if recordCodec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return nil, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed := make([]byte, envelopeHeaderSealed)
	if err := readAtFull(ra, sealed, envelopePrefixSize); err != nil {
		return nil, &canonicalReadError{fmt.Errorf("%w: read encrypted header", ErrMalformedEnvelope), err}
	}
	plaintext, err := recordCodec.DecryptInPlace(sealed, headerAAD(prefix), 0)
	if err != nil {
		return nil, &canonicalReadError{fmt.Errorf("%w: encrypted header", ErrAuthentication), err}
	}
	if len(plaintext) != envelopeHeaderSize || &plaintext[0] != &sealed[1] {
		return nil, fmt.Errorf("%w: codec violated in-place header contract", ErrMalformedEnvelope)
	}
	var plainHeader [envelopeHeaderSize]byte
	copy(plainHeader[:], plaintext)
	geometry, err := parseEnvelopeHeader(plainHeader[:])
	if err != nil {
		return nil, err
	}
	if err := validateCodecGeometry(recordCodec, geometry); err != nil {
		return nil, err
	}
	wantSize, err := geometry.physicalSize()
	if err != nil || wantSize != artifactSize {
		return nil, fmt.Errorf("%w: physical size mismatch", ErrMalformedEnvelope)
	}
	return &recordReaderAt{
		ra:       ra,
		codec:    recordCodec,
		prefix:   prefix,
		header:   plainHeader,
		geometry: geometry,
	}, nil
}

func validateCodecGeometry(codec RecordCodec, geometry envelopeHeader) error {
	lengths := []int{envelopeHeaderSize, int(geometry.firstSize), recordSize, int(geometry.recordPlainSize(geometry.recordCount - 1))}
	for _, length := range lengths {
		if length < 0 || codec.CiphertextSize(length) != length+recordOverhead {
			return fmt.Errorf("%w: codec ciphertext geometry", ErrMalformedEnvelope)
		}
	}
	return nil
}

func (h envelopeHeader) physicalSize() (int64, error) {
	overhead, overflow := multiplyUint64(h.recordCount, recordOverhead)
	if overflow {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	total, overflow := addUint64(encryptedDataOffset, h.plaintextSize)
	if overflow {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	total, overflow = addUint64(total, overhead)
	if overflow || total > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%w: physical size overflow", ErrMalformedEnvelope)
	}
	return int64(total), nil
}

func multiplyUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, true
	}
	return a * b, false
}

func (h envelopeHeader) recordPlainStart(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return uint64(h.firstSize) + (index-1)*recordSize
}

func (h envelopeHeader) recordPlainSize(index uint64) uint64 {
	start := h.recordPlainStart(index)
	remaining := h.plaintextSize - start
	target := uint64(recordSize)
	if index == 0 {
		target = uint64(h.firstSize)
	}
	if remaining < target {
		return remaining
	}
	return target
}

func (h envelopeHeader) recordForOffset(offset uint64) uint64 {
	if offset < uint64(h.firstSize) {
		return 0
	}
	return 1 + (offset-uint64(h.firstSize))/recordSize
}

func (h envelopeHeader) recordCipherOffset(index uint64) (int64, error) {
	plainStart := h.recordPlainStart(index)
	overhead, overflow := multiplyUint64(index, recordOverhead)
	if overflow {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	offset, overflow := addUint64(encryptedDataOffset, plainStart)
	if overflow {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	offset, overflow = addUint64(offset, overhead)
	if overflow || offset > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%w: record offset overflow", ErrMalformedEnvelope)
	}
	return int64(offset), nil
}

func (r *recordReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("%w: negative plaintext offset", ErrMalformedEnvelope)
	}
	if len(dst) == 0 {
		return 0, nil
	}
	plainOffset := uint64(offset)
	if plainOffset >= r.geometry.plaintextSize {
		return 0, io.EOF
	}
	want := len(dst)
	var eof error
	if uint64(want) > r.geometry.plaintextSize-plainOffset {
		want = int(r.geometry.plaintextSize - plainOffset)
		eof = io.EOF
	}
	written := 0
	for written < want {
		lastOffset := plainOffset + uint64(want-written) - 1
		last := r.geometry.recordForOffset(lastOffset)
		n, err := r.readBatch(dst[written:want], plainOffset, last)
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, readerr.Mark(io.ErrNoProgress, false)
		}
		written += n
		plainOffset += uint64(n)
	}
	return written, eof
}

func (r *recordReaderAt) checkOpen() error {
	if r.closed.Load() {
		return readerr.Mark(fmt.Errorf("tarstream: encrypted reader is closed"), false)
	}
	return nil
}

func (r *recordReaderAt) readBatch(dst []byte, plainOffset, last uint64) (int, error) {
	if err := r.checkOpen(); err != nil {
		return 0, err
	}
	first := r.geometry.recordForOffset(plainOffset)
	end := first
	total := 0
	for end <= last {
		cipherSize := int(r.geometry.recordPlainSize(end)) + recordOverhead
		if total > 0 && total+cipherSize > maxCoalesceBytes {
			break
		}
		total += cipherSize
		end++
	}
	if end == first {
		return 0, nil
	}
	physical, err := r.geometry.recordCipherOffset(first)
	if err != nil {
		return 0, err
	}
	work := acquireRecordReadBuffer(total + recordAADSize())
	defer releaseRecordReadBuffer(work)
	buffer := work.data
	slab := buffer[:total:total]
	if err := readAtFull(r.ra, slab, physical); err != nil {
		return 0, &canonicalReadError{fmt.Errorf("%w: truncated encrypted record", ErrMalformedEnvelope), err}
	}

	aad := buffer[total:]
	initRecordAAD(aad, r.prefix, r.header)
	position := 0
	written := 0
	for index := first; index < end; index++ {
		plainSize := int(r.geometry.recordPlainSize(index))
		cipherSize := plainSize + recordOverhead
		part := slab[position : position+cipherSize]
		if index == ^uint64(0) {
			return written, fmt.Errorf("%w: record sequence overflow", ErrMalformedEnvelope)
		}
		setRecordAAD(aad, index, uint32(plainSize))
		plaintext, err := r.codec.DecryptInPlace(part, aad, index+1)
		if err != nil {
			return written, &canonicalReadError{fmt.Errorf("%w: encrypted data record", ErrAuthentication), err}
		}
		if len(plaintext) != plainSize || plainSize > 0 && &plaintext[0] != &part[1] {
			return written, fmt.Errorf("%w: codec violated in-place record contract", ErrMalformedEnvelope)
		}
		recordStart := r.geometry.recordPlainStart(index)
		within := 0
		if plainOffset > recordStart {
			within = int(plainOffset - recordStart)
		}
		n := min(len(dst)-written, len(plaintext)-within)
		copy(dst[written:written+n], plaintext[within:within+n])
		written += n
		plainOffset += uint64(n)
		position += cipherSize
	}
	return written, nil
}

func acquireRecordReadBuffer(size int) *recordReadBuffer {
	index := recordReadBufferPoolIndex(size)
	if index < 0 {
		return &recordReadBuffer{data: make([]byte, size), poolIndex: -1}
	}
	if value := recordReadBufferPools[index].Get(); value != nil {
		buffer := value.(*recordReadBuffer)
		buffer.data = buffer.data[:size]
		return buffer
	}
	capacity := recordReadBufferCapacity(index)
	return &recordReadBuffer{
		data:      make([]byte, size, capacity),
		poolIndex: index,
	}
}

func releaseRecordReadBuffer(buffer *recordReadBuffer) {
	clear(buffer.data)
	if buffer.poolIndex < 0 {
		return
	}
	buffer.data = buffer.data[:cap(buffer.data)]
	recordReadBufferPools[buffer.poolIndex].Put(buffer)
}

func recordReadBufferPoolIndex(size int) int {
	if size <= 0 || size > maxCoalesceBytes+recordAADSize() {
		return -1
	}
	shift := bits.Len(uint(size - 1))
	if shift < recordReadBufferMinShift {
		shift = recordReadBufferMinShift
	}
	if shift > recordReadBufferMaxPowerShift {
		return recordReadBufferLargePool
	}
	return shift - recordReadBufferMinShift
}

func recordReadBufferCapacity(index int) int {
	if index == recordReadBufferLargePool {
		return maxCoalesceBytes + recordAADSize()
	}
	return 1 << (recordReadBufferMinShift + index)
}

func (r *recordReaderAt) Close() error {
	r.closed.Store(true)
	return nil
}

func readAtFull(reader io.ReaderAt, dst []byte, offset int64) error {
	if len(dst) == 0 {
		return nil
	}
	n, err := reader.ReadAt(dst, offset)
	if readerr.IsPermanent(err) {
		return err
	}
	if n < 0 || n > len(dst) {
		return readerr.Mark(fmt.Errorf("tarstream: invalid ReaderAt count %d: %w", n, io.ErrUnexpectedEOF), false)
	}
	if n == len(dst) && (err == nil || err == io.EOF) {
		return nil
	}
	if err == nil || err == io.EOF || err == io.ErrUnexpectedEOF {
		return readerr.Mark(fmt.Errorf("tarstream: read %d of %d bytes: %w", n, len(dst), io.ErrUnexpectedEOF), false)
	}
	return err
}
