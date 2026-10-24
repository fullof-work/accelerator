package tarstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// WriteTo packages src as a complete Kuasar tarstream artifact on w. The
// sparse map comes from src.RunAt — Hole runs become the entry's
// holes. Page-representable payloads include a full presence bitmap and a
// GNU sparse map of actual payload; whole Zero pages need no stored bytes.
// Other sources retain byte-exact holes and synthesize Zero bytes without
// calling ReadAt. Logical Zero always masks lower content. The metadata
// sweep precedes any data read (sparse law 1), so one-pass sources
// work. The plaintext envelope metadata is fixed and deterministic (mode 0644,
// uid/gid 0, epoch mtime): the entry is a transport vessel, not a
// filesystem snapshot. After the payload it appends one
// .kuasar.digest.<hex> entry. The returned carrier digest commits to the
// payload name, logical size, authoritative sparse map, payload bytes, and the
// dense E/S metadata tail declared by IdentityProvider. It is computed while the
// payload is written.
// With a codec the complete canonical stream, including marker and trailer,
// is placed inside encrypted v1 records.
func WriteTo(ctx context.Context, w io.Writer, name string, src sparse.Source, options ...WriteOption) (scheme string, digest string, err error) {
	opts, err := parseWriteOptions(options)
	if err != nil {
		return "", "", err
	}
	plan, err := makeWritePlanLimited(ctx, name, src, opts.indexMemory)
	if err != nil {
		return "", "", err
	}

	var artifactWriter io.Writer = w
	var records *recordWriter
	if opts.codec != nil {
		header, err := makeEnvelopeHeader(plan)
		if err != nil {
			return "", "", err
		}
		prefix, plainHeader, recordCodec, err := writeEncryptedHeader(w, opts.codec, header)
		if err != nil {
			return "", "", err
		}
		records = newRecordWriter(w, recordCodec, prefix, plainHeader, header)
		artifactWriter = records
	}

	plainDigest, err := emitPlan(ctx, artifactWriter, plan, src, opts.beforeCommit)
	if err != nil {
		return "", "", err
	}
	if records != nil {
		if err := records.Close(); err != nil {
			return "", "", err
		}
	}
	scheme, external := externalDigest(opts.codec, plainDigest)
	return scheme, hex.EncodeToString(external[:]), nil
}

type writePlan struct {
	name           string
	logical        int64
	stored         int64
	sparseMap      []byte
	extents        []extent
	prefix         []byte
	packedStart    int64
	packedSize     int64
	payloadPad     int
	plaintextSize  int64
	payloadSize    int64
	payloadPacked  int64
	payloadExtents []extent
	presence       []byte
}

func makeWritePlan(name string, src sparse.Source) (*writePlan, error) {
	return makeWritePlanLimited(context.Background(), name, src, 0)
}

func makeWritePlanLimited(ctx context.Context, name string, src sparse.Source, memoryLimit uint64) (*writePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = normalizeName(name)
	if name == "" {
		return nil, fmt.Errorf("tarstream: empty entry name")
	}
	if strings.HasPrefix(name, DigestMarkerPrefix) || name == presenceName {
		return nil, fmt.Errorf("tarstream: entry name %q uses reserved digest marker prefix", name)
	}
	if src.Size() > 1<<62 {
		return nil, fmt.Errorf("tarstream: %s: size %d overflows", name, src.Size())
	}
	size := int64(src.Size())
	// Conservative charge covers slice growth/copies, prefixExtents, decimal
	// map construction and its copied carrier prefix, plus payload buffers.
	indexMemory := uint64(2 << 20)
	charge := func(n uint64) error {
		if memoryLimit != 0 && (indexMemory > memoryLimit || n > memoryLimit-indexMemory) {
			return fmt.Errorf("tarstream: writer index memory limit exceeded")
		}
		indexMemory += n
		return nil
	}
	if err := charge(uint64(len(name)) * 8); err != nil {
		return nil, err
	}

	payloadSize := size
	if provider, ok := src.(IdentityProvider); ok {
		declared, _, _ := provider.PayloadCommitment()
		if declared > uint64(size) {
			return nil, fmt.Errorf("tarstream: payload exceeds logical size")
		}
		payloadSize = int64(declared)
	}
	presence, err := planPresence(ctx, src, uint64(payloadSize), charge)
	if err != nil {
		return nil, err
	}
	// Only actual payload is indexed in the new encoding. The presence bitmap
	// carries logical Zero; the business metadata tail remains dense.
	var extents []extent
	hasHole := presence != nil
	appendExtent := func(start, end uint64) error {
		if end <= start {
			return nil
		}
		if n := len(extents); n > 0 && extents[n-1].Offset+extents[n-1].Size == int64(start) {
			extents[n-1].Size += int64(end - start)
			return nil
		}
		if len(extents)+2 > maxSparseExtents {
			return fmt.Errorf("tarstream: sparse extent limit exceeded (including sentinel)")
		}
		if err := charge(512); err != nil {
			return err
		}
		extents = append(extents, extent{Offset: int64(start), Size: int64(end - start)})
		return nil
	}
	for off := uint64(0); off < uint64(size); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		run, err := src.RunAt(off, uint64(size)-off)
		if err != nil {
			return nil, err
		}
		kind, end := run.Kind(), run.End()
		if end <= off || end > uint64(size) {
			return nil, fmt.Errorf("tarstream: invalid source run")
		}
		switch kind {
		case sparse.Hole:
			hasHole = true
		case sparse.Data:
			if err := appendExtent(off, end); err != nil {
				return nil, err
			}
		case sparse.Zero:
			// Omit only complete Zero pages before the independent dense tail.
			zeroStart := (off + presencePage - 1) / presencePage * presencePage
			zeroEnd := min(end, uint64(payloadSize)) / presencePage * presencePage
			if presence != nil && zeroStart < zeroEnd {
				if err := appendExtent(off, zeroStart); err != nil {
					return nil, err
				}
				if err := appendExtent(zeroEnd, end); err != nil {
					return nil, err
				}
			} else if err := appendExtent(off, end); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("tarstream: invalid source kind")
		}
		off = end
	}
	var dataSize int64
	for _, e := range extents {
		if e.Size > 0 && dataSize > 1<<62-e.Size {
			return nil, fmt.Errorf("tarstream: %s: packed data size overflows", name)
		}
		dataSize += e.Size
	}

	payloadExtents, payloadPacked, tailDense := prefixExtents(extents, payloadSize, size)
	if !tailDense {
		return nil, fmt.Errorf("tarstream: %s: metadata tail [%d,%d) contains a hole", name, payloadSize, size)
	}
	if size-payloadSize > maxMetadataTailSize {
		return nil, fmt.Errorf("tarstream: %s: metadata tail size %d exceeds %d", name, size-payloadSize, maxMetadataTailSize)
	}
	if err := charge(uint64(size - payloadSize)); err != nil {
		return nil, err
	}

	var sparseMap []byte
	stored := size
	if hasHole {
		// Sparse map: decimal extent count, then offset/size per line —
		// the data extents plus GNU's trailing (size, 0) sentinel —
		// NUL-padded to a 512 boundary and prepended to the stored data.
		var mapBuf bytes.Buffer
		mapBuf.WriteString(strconv.Itoa(len(extents) + 1))
		mapBuf.WriteByte('\n')
		for _, e := range extents {
			mapBuf.WriteString(strconv.FormatInt(e.Offset, 10))
			mapBuf.WriteByte('\n')
			mapBuf.WriteString(strconv.FormatInt(e.Size, 10))
			mapBuf.WriteByte('\n')
		}
		mapBuf.WriteString(strconv.FormatInt(size, 10))
		mapBuf.WriteString("\n0\n")
		if pad := (512 - mapBuf.Len()%512) % 512; pad > 0 {
			mapBuf.Write(zeroBlock[:pad])
		}
		sparseMap = mapBuf.Bytes()
		if dataSize > 1<<62-int64(len(sparseMap)) {
			return nil, fmt.Errorf("tarstream: %s: stored size overflows", name)
		}
		stored = int64(len(sparseMap)) + dataSize
	}

	prefix := payloadPrefix(name, size, stored, sparseMap, payloadSize, presence != nil)
	if presence != nil {
		body := presenceBody(presence, uint64(payloadSize))
		h := ustarBlock(rawHeader{name: presenceName, mode: 0o444, size: int64(len(body)), typeflag: '0'})
		var full bytes.Buffer
		full.Grow(512 + (len(body)+511)/512*512 + len(prefix))
		full.Write(h[:])
		full.Write(body)
		if pad := (512 - len(body)%512) % 512; pad > 0 {
			full.Write(zeroBlock[:pad])
		}
		full.Write(prefix)
		prefix = full.Bytes()
	}
	payloadPad := int((512 - stored%512) % 512)
	plaintextSize := int64(len(prefix)) + dataSize + int64(payloadPad) + canonicalSuffixSize
	if plaintextSize < 0 || plaintextSize > 1<<62 {
		return nil, fmt.Errorf("tarstream: %s: artifact size overflows", name)
	}
	return &writePlan{
		name:           name,
		logical:        size,
		stored:         stored,
		sparseMap:      sparseMap,
		extents:        extents,
		prefix:         prefix,
		packedStart:    int64(len(prefix)),
		packedSize:     dataSize,
		payloadPad:     payloadPad,
		plaintextSize:  plaintextSize,
		payloadSize:    payloadSize,
		payloadPacked:  payloadPacked,
		payloadExtents: payloadExtents,
		presence:       presence,
	}, nil
}

// payloadPrefix renders all canonical bytes before the packed payload. The
// sparse map, when present, is part of this prefix.
func payloadPrefix(name string, logical, stored int64, sparseMap []byte, payloadSize int64, presence bool) []byte {
	headers := makePayloadHeaders(name, logical, stored, payloadSize, sparseMap != nil, presence)
	var prefix bytes.Buffer
	prefix.Write(headers.paxHeader[:])
	prefix.Write(headers.pax)
	if pad := (512 - len(headers.pax)%512) % 512; pad > 0 {
		prefix.Write(zeroBlock[:pad])
	}
	prefix.Write(headers.payloadHeader[:])
	if sparseMap != nil {
		prefix.Write(sparseMap)
	}
	return prefix.Bytes()
}

type payloadHeaders struct {
	records       map[string]string
	pax           []byte
	paxHeader     [512]byte
	payloadHeader [512]byte
}

// makePayloadHeaders is the single canonical transport-header definition used
// by both the encoder and the identity validator.
func makePayloadHeaders(name string, logical, stored, payloadSize int64, sparseEncoding, presence bool) payloadHeaders {
	ustarName := name
	records := map[string]string{
		"path":         name,
		"size":         strconv.FormatInt(stored, 10),
		payloadSizePAX: strconv.FormatInt(payloadSize, 10),
	}
	if presence {
		records[presencePAX] = "1"
	}
	if sparseEncoding {
		// GNU mangles the ustar name of sparse entries; readers recover the
		// real name from GNU.sparse.name. "0" replaces GNU's pid.
		ustarName = path.Join(path.Dir(name), "GNUSparseFile.0", path.Base(name))
		records["path"] = ustarName
		records["GNU.sparse.major"] = "1"
		records["GNU.sparse.minor"] = "0"
		records["GNU.sparse.name"] = name
		records["GNU.sparse.realsize"] = strconv.FormatInt(logical, 10)
	}
	pax := encodePAX(records)
	return payloadHeaders{
		records: records,
		pax:     pax,
		paxHeader: ustarBlock(rawHeader{
			name: "PaxHeaders.0/" + truncateStr(path.Base(name), 80), mode: 0o644,
			size: int64(len(pax)), typeflag: 'x',
		}),
		payloadHeader: ustarBlock(rawHeader{
			name: truncateStr(ustarName, 100), mode: 0o644, size: stored, typeflag: '0',
		}),
	}
}

// emitPlan writes the planned canonical plaintext stream exactly once. The
// destination may be the caller's plaintext writer or the encrypted record
// writer. Data extents are read once; Zero extents are synthesized.
func emitPlan(ctx context.Context, w io.Writer, plan *writePlan, src sparse.Source, beforeCommit func(context.Context) error) ([32]byte, error) {
	identity := newIdentityAccumulator(
		uint64(plan.payloadSize), plan.payloadExtents, plan.payloadPacked, plan.logical-plan.payloadSize, plan.presence, plan.extents,
	)
	if err := writeFull(w, plan.prefix); err != nil {
		return [32]byte{}, err
	}
	payloadWriter := io.MultiWriter(w, identity)
	if len(plan.extents) > 0 {
		buf := make([]byte, copyBufSize)
		for _, e := range plan.extents {
			if err := copyExtent(ctx, payloadWriter, src, e, plan.name, buf); err != nil {
				return [32]byte{}, err
			}
		}
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return [32]byte{}, fmt.Errorf("tarstream: %s: before commit: %w", plan.name, err)
		}
	}
	if plan.payloadPad > 0 {
		if err := writeFull(w, zeroBlock[:plan.payloadPad]); err != nil {
			return [32]byte{}, err
		}
	}
	computed := identity.finish(plan.name, uint64(plan.logical), uint64(plan.payloadSize))
	hexDigest := hex.EncodeToString(computed.digest[:])
	marker := ustarBlock(rawHeader{
		name:     DigestMarkerPrefix + hexDigest,
		mode:     0o444,
		size:     40,
		typeflag: '0',
	})
	if err := writeFull(w, marker[:]); err != nil {
		return [32]byte{}, err
	}
	var markerBody [512]byte
	binary.BigEndian.PutUint64(markerBody[:8], computed.payloadSize)
	copy(markerBody[8:40], computed.payload[:])
	if err := writeFull(w, markerBody[:]); err != nil {
		return [32]byte{}, err
	}
	// End of archive: two zero blocks.
	if err := writeFull(w, zeroBlock2[:]); err != nil {
		return [32]byte{}, err
	}
	return computed.digest, nil
}

const copyBufSize = 256 << 10

var (
	zeroBlock  [512]byte
	zeroBlock2 [1024]byte
)

// copyExtent writes the logical bytes of one data extent: Data runs
// are read from src, Zero runs are synthesized without a read. A
// short or failing source is a hard error — the header size is
// already committed, so the stream must fail loudly rather than be
// silently corrupt.
func copyExtent(ctx context.Context, w io.Writer, src sparse.Source, e extent, name string, buf []byte) error {
	off, end := uint64(e.Offset), uint64(e.Offset+e.Size)
	for off < end {
		run, err := src.RunAt(off, end-off)
		if err != nil {
			return fmt.Errorf("%s: classify @ %d: %w", name, off, err)
		}
		kind, runEnd := run.Kind(), run.End()
		if runEnd <= off || runEnd > end {
			return fmt.Errorf("%s: invalid run [%d,%d) inside data extent ending at %d", name, off, runEnd, end)
		}
		if kind != sparse.Zero && kind != sparse.Data && kind != sparse.Hole {
			return fmt.Errorf("%s: invalid run kind %d @ %d", name, kind, off)
		}
		if kind == sparse.Hole {
			return fmt.Errorf("%s: hole @ %d inside a data extent (inconsistent RunAt)", name, off)
		}
		for off < runEnd {
			n := len(buf)
			if rest := runEnd - off; rest < uint64(n) {
				n = int(rest)
			}
			chunk := buf[:n]
			if kind == sparse.Zero {
				clear(chunk)
			} else if m, err := run.ReadAt(ctx, chunk, off-run.Offset()); err != nil {
				return fmt.Errorf("%s: read @ %d: %w", name, off, err)
			} else if m != n {
				return fmt.Errorf("%s: source returned invalid length %d for %d bytes @ %d", name, m, n, off)
			}
			if err := writeFull(w, chunk); err != nil {
				return err
			}
			off += uint64(n)
		}
	}
	return nil
}

// encodePAX renders PAX records ("len key=value\n", sorted by key for
// deterministic output).
func encodePAX(recs map[string]string) []byte {
	keys := make([]string, 0, len(recs))
	for k := range recs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		v := recs[k]
		size := len(k) + len(v) + 3 // " " + "=" + "\n"
		size += len(strconv.Itoa(size))
		record := strconv.Itoa(size) + " " + k + "=" + v + "\n"
		if len(record) != size { // digit-count changed; re-render once
			size = len(record)
			record = strconv.Itoa(size) + " " + k + "=" + v + "\n"
		}
		buf.WriteString(record)
	}
	return buf.Bytes()
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// rawHeader carries the few ustar fields the raw encoder needs; PAX
// records override everything that matters on the reader side.
type rawHeader struct {
	name     string
	mode     int64
	size     int64
	typeflag byte
}

// ustarBlock encodes a 512-byte ustar header with a valid checksum.
// uid/gid/mtime are fixed to zero (deterministic transport envelope).
func ustarBlock(h rawHeader) [512]byte {
	var b [512]byte
	copy(b[0:100], h.name)
	octal(b[100:108], h.mode)
	octal(b[108:116], 0) // uid
	octal(b[116:124], 0) // gid
	octal(b[124:136], h.size)
	octal(b[136:148], 0) // mtime: epoch
	copy(b[148:156], "        ")
	b[156] = h.typeflag
	copy(b[257:263], "ustar\x00")
	copy(b[263:265], "00")
	var sum int64
	for _, c := range b {
		sum += int64(c)
	}
	s := strconv.FormatInt(sum, 8)
	for len(s) < 6 {
		s = "0" + s
	}
	copy(b[148:154], s)
	b[154] = 0
	b[155] = ' '
	return b
}

// octal writes a NUL-terminated octal field; out-of-range values
// become zero (the PAX record carries the real one).
func octal(dst []byte, v int64) {
	width := len(dst) - 1
	if v < 0 {
		v = 0
	}
	s := strconv.FormatInt(v, 8)
	if len(s) > width {
		s = "0"
	}
	for len(s) < width {
		s = "0" + s
	}
	copy(dst, s)
	dst[width] = 0
}
