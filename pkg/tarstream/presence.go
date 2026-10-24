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
	maxPresencePayload = uint64(32 << 30)
	maxPresenceBytes   = 1 << 20
	presenceHeaderSize = 64
	maxSparseExtents   = 1 << 20 // shared reader/writer wire limit, including sentinel
)

var presenceMagic = [8]byte{'K', 'U', 'P', 'R', 'E', 'S', '1', '\n'}

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

// planPresence performs metadata-only eligibility checking. Arbitrary byte Hole
// boundaries keep the legacy encoding; partial Zero runs stay ordinary Data.
// No payload bytes are read and no replacement format is used to evade budgets.
func planPresence(ctx context.Context, src sparse.Source, payload uint64, charge func(uint64) error) ([]byte, error) {
	if payload == 0 || payload%presencePage != 0 || payload > maxPresencePayload {
		return nil, nil
	}
	// Eligibility precedes allocation: byte-granular legacy holes do not pay
	// for a bitmap they cannot use. Charge all peak bitmap copies before make.
	var p []byte
	for pass := 0; pass < 2; pass++ {
		if pass == 1 {
			length := (payload/presencePage + 7) / 8
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
				if off%presencePage != 0 || end%presencePage != 0 {
					if pass == 0 {
						return nil, nil
					}
					return nil, fmt.Errorf("tarstream: source presence changed during planning")
				}
			case sparse.Data, sparse.Zero:
				if p != nil {
					setPresenceRange(p, off/presencePage, (end+presencePage-1)/presencePage)
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
		payload == 0 || payload > maxPresencePayload || payload%presencePage != 0 || length != (payload/presencePage+7)/8 || length != uint64(len(b)-presenceHeaderSize) || !isZeroBlock(b[32:60]) {
		return bad()
	}
	p := b[presenceHeaderSize:]
	h := crc32.NewIEEE()
	h.Write(b[:60])
	h.Write(p)
	if h.Sum32() != binary.LittleEndian.Uint32(b[60:]) {
		return bad()
	}
	if tail := payload / presencePage % 8; tail != 0 && p[len(p)-1]>>tail != 0 {
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
	for page := uint64(0); page < payload/presencePage; {
		end := presenceRunEnd(p, page, payload/presencePage)
		if presenceBit(p, page) {
			visit(page*presencePage, end*presencePage)
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
			pos = min(end, presenceRunEnd(p, page, (end+presencePage-1)/presencePage)*presencePage)
		}
	}
	return nil
}

func (m *meta) logicalHoles() []sparse.Extent {
	if m.presence == nil {
		return append([]sparse.Extent(nil), m.holes...)
	}
	var holes []sparse.Extent
	for page := uint64(0); page < uint64(m.payloadSize)/presencePage; {
		end := presenceRunEnd(m.presence, page, uint64(m.payloadSize)/presencePage)
		if !presenceBit(m.presence, page) {
			holes = append(holes, sparse.Extent{Offset: page * presencePage, Size: (end - page) * presencePage})
		}
		page = end
	}
	return holes
}
