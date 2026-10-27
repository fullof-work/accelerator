package tarstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const (
	presenceName       = ".kuasar.presence.v1"
	presencePAX        = "KUASAR.presence"
	presencePage       = uint64(4096)
	maxPresencePayload = uint64(1 << 40)
	maxPresenceBytes   = 32 << 20
	presenceHeaderSize = 64
	maxSparseExtents   = 1 << 20 // shared reader/writer wire limit, including sentinel
)

var presenceMagic = [8]byte{'K', 'U', 'P', 'R', 'E', 'S', '1', '\n'}

func presencePages(payload uint64) uint64 {
	pages := payload / presencePage
	if payload%presencePage != 0 {
		pages++
	}
	return pages
}

func presenceBit(p []byte, page uint64) bool { return p[page/8]&(1<<(page%8)) != 0 }
func presenceRunEnd(p []byte, page, pages uint64) uint64 {
	set := presenceBit(p, page)
	page++
	for page < pages {
		if page%8 == 0 && page+8 <= pages {
			v := byte(0)
			if set {
				v = 255
			}
			if p[page/8] == v {
				page += 8
				continue
			}
		}
		if presenceBit(p, page) != set {
			break
		}
		page++
	}
	return page
}
func setPresenceRange(p []byte, start, end uint64) {
	for start < end && start%8 != 0 {
		p[start/8] |= 1 << (start % 8)
		start++
	}
	for start+8 <= end {
		p[start/8] = 255
		start += 8
	}
	for start < end {
		p[start/8] |= 1 << (start % 8)
		start++
	}
}

// planPresence validates and plans the required presence profile using only
// metadata. Inputs that cannot preserve their exact length and Hole boundaries
// in this profile fail before output; partial Zero runs stay ordinary Data.
func planPresence(ctx context.Context, src sparse.Source, payload uint64, charge func(uint64) error) ([]byte, error) {
	if payload > maxPresencePayload {
		return nil, fmt.Errorf("tarstream: payload size %d exceeds presence limit %d", payload, maxPresencePayload)
	}
	// Validate all Hole boundaries before allocating the bitmap. Charge all
	// peak bitmap copies before make.
	var p []byte
	for pass := 0; pass < 2; pass++ {
		if pass == 1 {
			length := (presencePages(payload) + 7) / 8
			if err := charge(length*4 + 4096); err != nil {
				return nil, err
			}
			p = make([]byte, length)
		}
		for off := uint64(0); off < payload; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			r, err := src.RunAt(off, payload-off)
			if err != nil {
				return nil, err
			}
			end := r.End()
			if end <= off || end > payload {
				return nil, fmt.Errorf("tarstream: invalid presence run")
			}
			switch r.Kind() {
			case sparse.Hole:
				if off%presencePage != 0 || (end != payload && end%presencePage != 0) {
					if pass == 0 {
						return nil, fmt.Errorf("tarstream: presence hole [%d,%d) must have %d-byte aligned boundaries except at payload end", off, end, presencePage)
					}
					return nil, fmt.Errorf("tarstream: source presence changed during planning")
				}
			case sparse.Data, sparse.Zero:
				if p != nil {
					setPresenceRange(p, off/presencePage, presencePages(end))
				}
			default:
				return nil, fmt.Errorf("tarstream: invalid presence kind")
			}
			off = end
		}
	}
	return p, nil
}

func presenceBody(p []byte, payload uint64) []byte {
	b := make([]byte, presenceHeaderSize+len(p))
	copy(b, presenceMagic[:])
	binary.LittleEndian.PutUint32(b[8:], 1)
	binary.LittleEndian.PutUint32(b[12:], uint32(presencePage))
	binary.LittleEndian.PutUint64(b[16:], payload)
	binary.LittleEndian.PutUint64(b[24:], uint64(len(p)))
	copy(b[presenceHeaderSize:], p)
	h := crc32.NewIEEE()
	h.Write(b[:60])
	h.Write(p)
	binary.LittleEndian.PutUint32(b[60:], h.Sum32())
	return b
}

func readPresence(r io.Reader, stored int64) ([]byte, uint64, error) {
	bad := func() ([]byte, uint64, error) {
		return nil, 0, fmt.Errorf("%w: invalid presence entry", ErrInvalidCanonicalTarstream)
	}
	if stored < presenceHeaderSize || stored > presenceHeaderSize+maxPresenceBytes {
		return bad()
	}
	b := make([]byte, stored)
	if _, err := readFixed(r, b); err != nil {
		return nil, 0, err
	}
	payload := binary.LittleEndian.Uint64(b[16:])
	length := binary.LittleEndian.Uint64(b[24:])
	if !bytes.Equal(b[:8], presenceMagic[:]) || binary.LittleEndian.Uint32(b[8:]) != 1 || binary.LittleEndian.Uint32(b[12:]) != uint32(presencePage) ||
		payload > maxPresencePayload || length != (presencePages(payload)+7)/8 || length != uint64(len(b)-presenceHeaderSize) || !isZeroBlock(b[32:60]) {
		return bad()
	}
	p := b[presenceHeaderSize:]
	h := crc32.NewIEEE()
	h.Write(b[:60])
	h.Write(p)
	if h.Sum32() != binary.LittleEndian.Uint32(b[60:]) {
		return bad()
	}
	if tail := presencePages(payload) % 8; tail != 0 && p[len(p)-1]>>tail != 0 {
		return bad()
	}
	var padding [512]byte
	n := (512 - stored%512) % 512
	if _, err := readFixed(r, padding[:n]); err != nil {
		return nil, 0, err
	}
	if !isZeroBlock(padding[:n]) {
		return bad()
	}
	return p, payload, nil
}

// Walk logical present intervals without allocating an interval/prefix index.
func walkPresence(p []byte, payload uint64, visit func(uint64, uint64)) {
	pages := presencePages(payload)
	for page := uint64(0); page < pages; {
		end := presenceRunEnd(p, page, pages)
		if presenceBit(p, page) {
			visit(page*presencePage, min(end*presencePage, payload))
		}
		page = end
	}
}
func presencePayloadHasher(payload uint64, p []byte) (hash.Hash, int64) {
	var count, total uint64
	walkPresence(p, payload, func(start, end uint64) { count++; total += end - start })
	h := newPayloadHasherHeader(payload, count)
	walkPresence(p, payload, func(start, end uint64) { writeHashUint64(h, start); writeHashUint64(h, end-start) })
	return h, int64(total)
}

// Validate that every stored byte in the disk payload belongs to a present
// page. Only payload extents are checked; a dense business tail is independent.
func validatePresenceExtents(p []byte, payload uint64, extents []extent) error {
	for _, e := range extents {
		end := min(uint64(e.Offset+e.Size), payload)
		for pos := uint64(e.Offset); pos < end; {
			page := pos / presencePage
			if !presenceBit(p, page) {
				return fmt.Errorf("%w: stored payload in absent page", ErrInvalidCanonicalTarstream)
			}
			pos = min(end, presenceRunEnd(p, page, presencePages(end))*presencePage)
		}
	}
	return nil
}

func (m *meta) logicalHoles() []sparse.Extent {
	if m.presence == nil {
		return append([]sparse.Extent(nil), m.holes...)
	}
	var holes []sparse.Extent
	payload := uint64(m.payloadSize)
	pages := presencePages(payload)
	for page := uint64(0); page < pages; {
		end := presenceRunEnd(m.presence, page, pages)
		if !presenceBit(m.presence, page) {
			start := page * presencePage
			holes = append(holes, sparse.Extent{Offset: start, Size: min(end*presencePage, payload) - start})
		}
		page = end
	}
	return holes
}
