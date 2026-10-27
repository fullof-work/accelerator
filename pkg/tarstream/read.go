package tarstream

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// asReadSeeker returns r as an io.ReadSeeker only if it BOTH implements the
// interface AND can actually seek. An *os.File satisfies io.ReadSeeker even when
// its fd is a pipe/socket, where Seek fails at runtime with ESPIPE ("illegal
// seek") — e.g. a piped stdin (`cat foo | manifest-ctl store`). A no-op
// current-offset probe distinguishes a real seekable (regular file) from a stream,
// so the latter takes the one-pass path instead of erroring.
func asReadSeeker(r io.Reader) (io.ReadSeeker, bool) {
	rs, ok := r.(io.ReadSeeker)
	if !ok {
		return nil, false
	}
	if _, err := rs.Seek(0, io.SeekCurrent); err != nil {
		return nil, false
	}
	return rs, true
}

// ReadFrom locates the entry called name in the tar stream r (an empty
// name takes the first regular file entry) and returns its logical
// view: Read yields size bytes with holes reading as zeros, pulling
// only the packed data from r. When r is actually seekable the call
// upgrades to ReadSeekFrom — the returned Reader then also implements
// ReadSeeker (type-assert to use it). Otherwise the view is one
// sequential pass with nothing buffered beyond a block.
func ReadFrom(r io.Reader, name string) (Reader, error) {
	if rs, ok := asReadSeeker(r); ok {
		v, err := newSeekView(rs, name)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	v, err := newSeqView(r, name)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// ReadSeekFrom is ReadFrom over a seekable stream: the returned view
// supports random access by mapping logical offsets straight onto the
// packed data region inside rs — no extraction, no copies. The view
// owns rs's seek position; do not use rs elsewhere while reading.
func ReadSeekFrom(rs io.ReadSeeker, name string) (ReadSeeker, error) {
	v, err := newSeekView(rs, name)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// SourceFrom locates the entry called name (empty: first user regular file
// entry) and opens it as a sparse.Source — the pipeline-facing twin
// of ReadFrom: RunAt serves Hole/Zero/Data from validated presence metadata,
// or the exact legacy Hole/Data map. ReadAt returns logical bytes. Over a plain reader the
// source is one-pass (monotone ReadAt), even when r also implements Seek. This
// preserves the full-consumption boundary: the final Data read drains and
// validates the marker, trailer, expected digest, and outer EOF, including a
// trailing Zero/Hole region. With no Data this validation completes at open.
// The entry's name is returned alongside.
func SourceFrom(r io.Reader, name string, options ...ReadOption) (sparse.Source, string, error) {
	opts, err := parseReadOptions(options)
	if err != nil {
		return nil, "", err
	}
	return sourceFromSequential(r, name, opts)
}

// ReadSeekFromIndex is ReadSeekFrom addressing the entry by ordinal
// instead of name: index counts the real entries of the archive in
// order (meta entries — PAX, GNU longname/longlink, global headers —
// are not counted), exactly the sequence archive/tar's Reader.Next
// yields. It exists so a consumer iterating an archive with the
// stdlib reader can re-locate the Nth member here and recover what
// the stdlib hides (the sparse hole map).
func ReadSeekFromIndex(rs io.ReadSeeker, index int) (ReadSeeker, error) {
	m, err := locateAt(rs, index, seekSkip(rs))
	if err != nil {
		return nil, err
	}
	return seekViewFrom(rs, m)
}

// SourceAt opens plaintext canonical tarstreams and encrypted v1 envelopes as
// concurrent random-access sparse sources. A full encrypted magic match is a
// commitment to encrypted parsing: no authenticated or format failure falls
// back to plaintext. Encrypted open authenticates the header and suffix digest
// declaration, not every data record or its membership in that declaration;
// callers needing full-file validation must consume SourceFrom instead.
func SourceAt(ra io.ReaderAt, size int64, name string, options ...ReadOption) (sparse.Source, string, error) {
	if size < 0 || size > 1<<62 {
		return nil, "", readerr.Mark(fmt.Errorf("tarstream: invalid artifact size %d", size), false)
	}
	ra = artifactReaderAt{ra}

	opts, err := parseReadOptions(options)
	if err != nil {
		return nil, "", err
	}
	encrypted, err := encryptedMagicAt(ra, size)
	if err != nil {
		return nil, "", err
	}
	if encrypted {
		if opts.codec == nil {
			return nil, "", ErrCodecRequired
		}
		records, err := openRecordReaderAt(ra, size, opts.codec)
		if err != nil {
			return nil, "", keyBoundCanonicalError(opts, err)
		}
		result, err := sourceAtPlain(records, int64(records.geometry.plaintextSize), name)
		if err != nil {
			_ = records.Close()
			return nil, "", keyBoundCanonicalError(opts, err)
		}
		if !result.hasDigest {
			_ = records.Close()
			return nil, "", ErrInvalidCanonicalTarstream
		}
		packedSize := result.meta.stored - result.meta.mapLen
		if result.dataStart < 0 || packedSize < 0 || uint64(result.dataStart) != records.geometry.packedStart || uint64(packedSize) != records.geometry.packedSize {
			_ = records.Close()
			return nil, "", fmt.Errorf("%w: inner tar layout does not match authenticated header", ErrMalformedEnvelope)
		}
		scheme, digest := externalDigest(opts.codec, result.identity.digest)
		if err := checkExpected(opts, scheme, digest); err != nil {
			_ = records.Close()
			return nil, "", err
		}
		return &encryptedDigestedSource{Source: result.source, records: records, name: result.name, identity: result.identity, scheme: scheme, digest: hex.EncodeToString(digest[:])}, result.name, nil
	}
	if opts.required {
		return nil, "", ErrPlaintextForbidden
	}
	result, err := sourceAtPlain(ra, size, name)
	if err != nil {
		return nil, "", keyBoundCanonicalError(opts, err)
	}
	if !result.hasDigest {
		if opts.codec != nil || opts.expectedSet {
			return nil, "", ErrInvalidCanonicalTarstream
		}
		return result.source, result.name, nil
	}
	// A plaintext carrier keeps its plaintext identity even when an auto-mode
	// codec is available. The physical carrier, not ambient policy, supplies
	// the digest value.
	scheme, digest := externalDigest(nil, result.identity.digest)
	if err := checkExpected(opts, scheme, digest); err != nil {
		return nil, "", err
	}
	return &digestedSource{Source: result.source, name: result.name, identity: result.identity, scheme: scheme, digest: hex.EncodeToString(digest[:])}, result.name, nil
}

// keyBoundCanonicalError prevents parser diagnostics derived from decrypted or
// auto-mode plaintext metadata from crossing the key-bound API boundary. Safe,
// value-free sentinel categories remain distinguishable with errors.Is.
func keyBoundCanonicalError(options readOptions, err error) error {
	if options.codec == nil {
		return err
	}
	for _, safe := range []error{
		ErrAuthentication,
		ErrMalformedEnvelope,
		ErrUnsupportedVersion,
		ErrUnsupportedEncoding,
		ErrNotFound,
		ErrInvalidCanonicalTarstream,
	} {
		if errors.Is(err, safe) {
			return &canonicalReadError{safe, err}
		}
	}
	return &canonicalReadError{ErrInvalidCanonicalTarstream, err}
}

// Keep the existing value-free diagnostic while retaining the actual cause.
type canonicalReadError struct{ category, cause error }

func (e *canonicalReadError) Error() string        { return e.category.Error() }
func (e *canonicalReadError) Unwrap() error        { return e.cause }
func (e *canonicalReadError) Is(target error) bool { return errors.Is(e.category, target) }

type plainAtResult struct {
	source    sparse.Source
	name      string
	meta      *meta
	dataStart int64
	identity  carrierIdentity
	hasDigest bool
}

func sourceAtPlain(ra io.ReaderAt, size int64, name string) (result plainAtResult, retErr error) {
	defer func() {
		if retErr == io.EOF || retErr == io.ErrUnexpectedEOF {
			retErr = readerr.Mark(retErr, false)
		}
	}()
	// TAR parsing makes small reads within the same authenticated metadata
	// record. Keep one record only while opening; payload reads retain their
	// existing uncached ReaderAt contract.
	metadata := ra
	if records, ok := ra.(*recordReaderAt); ok {
		window := &recordMetadataReader{records: records, index: ^uint64(0)}
		defer clear(window.buffer[:])
		metadata = window
	}
	sr := io.NewSectionReader(metadata, 0, size)
	m, err := locate(sr, name, seekSkip(sr))
	if err != nil {
		return plainAtResult{}, err
	}
	dataStart, err := sr.Seek(0, io.SeekCurrent)
	if err != nil {
		return plainAtResult{}, err
	}
	src := sparse.Source(&readerAtSource{
		meta:         *m,
		ra:           ra,
		dataStart:    dataStart,
		packedPrefix: packedPrefix(m.extents),
	})
	identity, ok, err := discoverDigest(metadata, size, m, dataStart)
	if err != nil {
		return plainAtResult{}, err
	}
	return plainAtResult{source: src, name: m.name, meta: m, dataStart: dataStart, identity: identity, hasDigest: ok}, nil
}

// digestedSource adds the optional, already-parsed artifact identity without
// changing sparse.Source or performing I/O from Digest.
type digestedSource struct {
	sparse.Source
	name     string
	identity carrierIdentity
	scheme   string
	digest   string
}

func (s *digestedSource) Digest() (string, string) { return s.scheme, s.digest }
func (s *digestedSource) TarStreamDigest(name string) ([32]byte, bool) {
	return s.identity.digest, normalizeName(name) == s.name
}
func (s *digestedSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.identity.payloadSize, s.identity.payload, true
}

type encryptedDigestedSource struct {
	sparse.Source
	records  *recordReaderAt
	name     string
	identity carrierIdentity
	scheme   string
	digest   string
}

func (s *encryptedDigestedSource) Digest() (string, string) { return s.scheme, s.digest }
func (s *encryptedDigestedSource) TarStreamDigest(name string) ([32]byte, bool) {
	return s.identity.digest, normalizeName(name) == s.name
}
func (s *encryptedDigestedSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.identity.payloadSize, s.identity.payload, true
}
func (s *encryptedDigestedSource) Close() error { return s.records.Close() }

func encryptedMagicAt(ra io.ReaderAt, size int64) (bool, error) {
	if size < int64(len(envelopeMagic)) {
		return false, nil
	}
	var magic [8]byte
	if err := readAtFull(ra, magic[:], 0); err != nil {
		return false, err
	}
	return magic == envelopeMagic, nil
}

// discoverDigest recognizes the strict payload-plus-marker artifact shape. A normal tar
// without a marker is not an error; a reserved marker that is present but
// malformed is. All scans skip stored payload bytes with Seek.
func discoverDigest(ra io.ReaderAt, size int64, first *meta, dataStart int64) (carrierIdentity, bool, error) {
	var zero carrierIdentity
	packedSize := first.stored - first.mapLen
	if dataStart < 0 || packedSize < 0 || dataStart > size || packedSize > size-dataStart {
		return zero, false, fmt.Errorf("%w: invalid payload bounds", ErrInvalidCanonicalTarstream)
	}
	expectedMarkerStart := dataStart + packedSize
	padding := (512 - first.stored%512) % 512
	if padding > size-expectedMarkerStart {
		return zero, false, fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream)
	}
	if padding > 0 {
		var payloadPadding [512]byte
		if err := readAtFull(ra, payloadPadding[:padding], expectedMarkerStart); err != nil {
			return zero, false, fmt.Errorf("tarstream: read payload padding: %w", err)
		}
		if !isZeroBlock(payloadPadding[:padding]) {
			return zero, false, fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream)
		}
	}
	expectedMarkerStart += padding

	markerReader := io.NewSectionReader(ra, expectedMarkerStart, size-expectedMarkerStart)
	marker, err := locateAt(markerReader, 0, seekSkip(markerReader))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if first.presence != nil {
				return zero, false, ErrInvalidCanonicalTarstream
			}
			return zero, false, nil
		}
		return zero, false, err
	}
	if !strings.HasPrefix(marker.name, DigestMarkerPrefix) {
		if first.presence != nil {
			return zero, false, ErrInvalidCanonicalTarstream
		}
		return zero, false, nil
	}
	if first.ordinal != 0 && !(first.ordinal == 1 && first.presence != nil) {
		return zero, false, fmt.Errorf("%w: payload must be the first archive entry", ErrInvalidCanonicalTarstream)
	}
	if marker.logical != 40 || marker.stored != 40 || marker.mapLen != 0 {
		return zero, false, fmt.Errorf("%w: invalid digest marker body", ErrInvalidCanonicalTarstream)
	}
	markerLength, err := markerReader.Seek(0, io.SeekCurrent)
	if err != nil {
		return zero, false, err
	}
	markerEnd := expectedMarkerStart + markerLength + 512
	if markerLength != 512 || markerEnd > size || size-markerEnd != int64(len(zeroBlock2)) {
		return zero, false, fmt.Errorf("%w: digest marker must be the final entry", ErrInvalidCanonicalTarstream)
	}
	var markerBlock [512]byte
	if err := readAtFull(ra, markerBlock[:], expectedMarkerStart); err != nil {
		return zero, false, fmt.Errorf("tarstream: read digest marker: %w", err)
	}
	var markerBody [512]byte
	if err := readAtFull(ra, markerBody[:], expectedMarkerStart+512); err != nil {
		return zero, false, fmt.Errorf("tarstream: read digest marker body: %w", err)
	}
	identity, err := parseCanonicalMarker(markerBlock[:], markerBody[:])
	if err != nil {
		return zero, false, err
	}
	if !first.hasPayloadSize || first.payloadSize < 0 || uint64(first.payloadSize) != identity.payloadSize {
		return zero, false, fmt.Errorf("%w: payload size metadata mismatch", ErrInvalidCanonicalTarstream)
	}
	if !first.canonicalPayload {
		return zero, false, fmt.Errorf("%w: non-canonical payload header metadata", ErrInvalidCanonicalTarstream)
	}
	_, payloadPacked, dense := prefixExtents(first.extents, first.payloadSize, first.logical)
	if !dense {
		return zero, false, fmt.Errorf("%w: metadata tail contains a hole", ErrInvalidCanonicalTarstream)
	}
	tailSize := first.logical - first.payloadSize
	if tailSize > maxMetadataTailSize {
		return zero, false, fmt.Errorf("%w: metadata tail size %d exceeds %d", ErrInvalidCanonicalTarstream, tailSize, maxMetadataTailSize)
	}
	tailHasher := newTailHasher(uint64(tailSize))
	copied, err := io.Copy(tailHasher, io.NewSectionReader(ra, dataStart+payloadPacked, tailSize))
	if err != nil {
		return zero, false, fmt.Errorf("tarstream: read metadata tail: %w", err)
	}
	if copied != tailSize {
		return zero, false, readerr.Mark(fmt.Errorf("tarstream: short metadata tail: %w", io.ErrUnexpectedEOF), false)
	}
	var tailDigest [32]byte
	copy(tailDigest[:], tailHasher.Sum(nil))
	composed := composeDigest(first.name, uint64(first.logical), uint64(first.payloadSize), identity.payload, tailDigest)
	if composed != identity.digest {
		return zero, false, ErrDigestMismatch
	}
	var trailer [1024]byte
	if err := readAtFull(ra, trailer[:], markerEnd); err != nil {
		return zero, false, fmt.Errorf("tarstream: read end-of-archive: %w", err)
	}
	if !isZeroBlock(trailer[:512]) || !isZeroBlock(trailer[512:]) {
		return zero, false, fmt.Errorf("%w: invalid end-of-archive", ErrInvalidCanonicalTarstream)
	}
	return identity, true, nil
}

func newSeqView(r io.Reader, name string) (*seqView, error) {
	m, err := locate(r, name, func(n int64) error {
		_, err := io.CopyN(io.Discard, r, n)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &seqView{
		meta: *m,
		src:  io.LimitReader(r, m.stored-m.mapLen),
	}, nil
}

func newSeekView(rs io.ReadSeeker, name string) (*seekView, error) {
	m, err := locate(rs, name, seekSkip(rs))
	if err != nil {
		return nil, err
	}
	return seekViewFrom(rs, m)
}

// seekSkip returns a locate skip function that advances rs in place.
func seekSkip(rs io.Seeker) func(int64) error {
	return func(n int64) error {
		_, err := rs.Seek(n, io.SeekCurrent)
		return err
	}
}

// seekViewFrom builds the random-access view for an entry locate has
// just positioned rs at (start of packed data).
func seekViewFrom(rs io.ReadSeeker, m *meta) (*seekView, error) {
	dataStart, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return &seekView{meta: *m, rs: rs, dataStart: dataStart, packedPrefix: packedPrefix(m.extents)}, nil
}

// packedPrefix returns prefix sums of extent sizes: packedPrefix[i] is
// the packed-region offset where extent i's bytes begin.
func packedPrefix(extents []extent) []int64 {
	p := make([]int64, len(extents)+1)
	for i, e := range extents {
		p[i+1] = p[i] + e.Size
	}
	return p
}

// meta describes the located entry.
type meta struct {
	name             string
	ordinal          int
	logical          int64           // logical file size
	stored           int64           // stored entry size (map + packed data)
	mapLen           int64           // sparse map bytes consumed from the stored region
	extents          []extent        // data extents in logical offsets (dense: one run)
	holes            []sparse.Extent // canonical hole map (nil = dense)
	payloadSize      int64
	hasPayloadSize   bool
	canonicalPayload bool
	presence         []byte
}

// locate scans entries until it finds the one named want (empty: the
// first regular file entry), leaving r positioned at the start of its
// packed data (the sparse map, when present, has been consumed). skip
// advances r across unselected content.
func locate(r io.Reader, want string, skip func(n int64) error) (*meta, error) {
	want = normalizeName(want)
	return locateMatch(r, skip, false, func(name string, _ int, regular bool) bool {
		return name == want || (want == "" && regular)
	})
}

// locateAt scans entries until the index-th real entry (0-based; meta
// entries are not counted — the same sequence archive/tar yields).
func locateAt(r io.Reader, index int, skip func(n int64) error) (*meta, error) {
	return locateMatch(r, skip, true, func(_ string, ordinal int, _ bool) bool {
		return ordinal == index
	})
}

// locateMatch is the scanning core behind locate/locateAt: match is
// consulted once per real entry with its effective name, ordinal and
// regular-ness; the first match must be a regular file.
func locateMatch(r io.Reader, skip func(n int64) error, includeInternal bool, match func(name string, ordinal int, regular bool) bool) (*meta, error) {
	var blk [512]byte
	pax := map[string]string{}
	longName := ""
	sawZero := false
	ordinal := 0
	canonicalMeta := true
	var paxHeader [512]byte
	paxHeaderSeen := false
	var pendingPresence []byte
	var presencePayload uint64
	seenPresence := false
	for {
		if _, err := readFixed(r, blk[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, ErrNotFound
			}
			return nil, err
		}
		if isZeroBlock(blk[:]) {
			if sawZero {
				if pendingPresence != nil {
					return nil, fmt.Errorf("%w: presence without payload", ErrInvalidCanonicalTarstream)
				}
				return nil, ErrNotFound // end of archive
			}
			sawZero = true
			continue
		}
		if sawZero {
			canonicalMeta = false
		}
		sawZero = false
		if string(blk[257:262]) != "ustar" {
			return nil, readerr.Mark(fmt.Errorf("tarstream: not a tar header (bad magic)"), false)
		}
		if !validHeaderChecksum(blk[:]) {
			return nil, readerr.Mark(fmt.Errorf("tarstream: not a tar header (bad checksum)"), false)
		}

		typeflag := blk[156]
		size, err := parseNumeric(blk[124:136])
		if err != nil {
			return nil, readerr.Mark(fmt.Errorf("tarstream: bad size field: %w", err), false)
		}
		if size < 0 || size > 1<<62 {
			return nil, readerr.Mark(fmt.Errorf("tarstream: bad size field %d", size), false)
		}
		padded := (size + 511) &^ 511

		switch typeflag {
		case 'x': // PAX extended header for the next entry
			recs, canonicalPAX, err := readPAX(r, size, padded)
			if err != nil {
				return nil, err
			}
			if paxHeaderSeen || !canonicalPAX {
				canonicalMeta = false
			}
			paxHeader = blk
			paxHeaderSeen = true
			maps.Copy(pax, recs)
			continue
		case 'g': // global header: not interpreted
			canonicalMeta = false
			if err := skip(padded); err != nil {
				return nil, err
			}
			continue
		case 'L': // GNU longname: data is the next entry's name
			canonicalMeta = false
			if size > 1<<20 {
				return nil, readerr.Mark(fmt.Errorf("tarstream: absurd GNU longname size %d", size), false)
			}
			data := make([]byte, size)
			if _, err := readFixed(r, data); err != nil {
				return nil, err
			}
			if err := skip(padded - size); err != nil {
				return nil, err
			}
			longName = strings.TrimRight(string(data), "\x00")
			continue
		case 'K': // GNU longlink: irrelevant here
			canonicalMeta = false
			if err := skip(padded); err != nil {
				return nil, err
			}
			continue
		case 'S': // legacy GNU binary sparse: cannot even be skipped safely
			return nil, ErrUnsupportedEncoding
		}

		// A real entry: resolve its effective name and stored size.
		effName := pax["GNU.sparse.name"]
		if effName == "" {
			effName = pax["path"]
		}
		if effName == "" {
			effName = longName
		}
		if effName == "" {
			effName = ustarName(blk[:])
		}
		effName = normalizeName(effName)
		stored := size
		if s, ok := pax["size"]; ok {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, readerr.Mark(fmt.Errorf("tarstream: bad pax size: %w", err), false)
			}
			if v < 0 || v > 1<<62 {
				return nil, readerr.Mark(fmt.Errorf("tarstream: bad pax size %d", v), false)
			}
			stored = v
		}
		entryPAX := pax
		entryCanonicalMeta := canonicalMeta
		entryPAXHeader := paxHeader
		entryPAXHeaderSeen := paxHeaderSeen
		pax = map[string]string{}
		longName = ""
		canonicalMeta = true
		paxHeader = [512]byte{}
		paxHeaderSeen = false

		regular := typeflag == '0' || typeflag == 0
		matched := match(effName, ordinal, regular)
		ordinal++
		if effName == presenceName {
			h := ustarBlock(rawHeader{name: presenceName, mode: 0o444, size: stored, typeflag: '0'})
			if seenPresence || !regular || !entryCanonicalMeta || entryPAXHeaderSeen || !bytes.Equal(blk[:], h[:]) {
				return nil, fmt.Errorf("%w: duplicate or invalid presence header", ErrInvalidCanonicalTarstream)
			}
			seenPresence = true
			pendingPresence, presencePayload, err = readPresence(r, stored)
			if err != nil {
				return nil, err
			}
			if matched && includeInternal {
				// Index APIs follow archive/tar's physical entry ordinal. The
				// internal member remains addressable after validation.
				if err := skip(-((stored + 511) &^ 511)); err != nil {
					return nil, err
				}
				return &meta{name: effName, ordinal: ordinal - 1, logical: stored, stored: stored, extents: []extent{{Size: stored}}}, nil
			}
			continue
		}
		mark, marked := entryPAX[presencePAX]
		if marked && (mark != "1" || pendingPresence == nil) || !marked && pendingPresence != nil {
			return nil, fmt.Errorf("%w: presence marker mismatch", ErrInvalidCanonicalTarstream)
		}
		entryPresence := pendingPresence
		pendingPresence = nil
		if !matched && entryPresence == nil {
			if err := skip((stored + 511) &^ 511); err != nil {
				return nil, err
			}
			continue
		}
		if !regular {
			return nil, readerr.Mark(fmt.Errorf("tarstream: entry %q is not a regular file", effName), false)
		}
		if _, old := entryPAX["GNU.sparse.numblocks"]; old {
			return nil, ErrUnsupportedEncoding
		}
		if _, old := entryPAX["GNU.sparse.map"]; old {
			return nil, ErrUnsupportedEncoding
		}

		payloadValue, hasPayloadSize := entryPAX[payloadSizePAX]
		if entryPresence != nil && (!hasPayloadSize || entryPAX["GNU.sparse.major"] != "1" || entryPAX["GNU.sparse.minor"] != "0") {
			return nil, ErrInvalidCanonicalTarstream
		}
		if entryPAX["GNU.sparse.major"] == "1" && entryPAX["GNU.sparse.minor"] == "0" {
			realsize, err := strconv.ParseInt(entryPAX["GNU.sparse.realsize"], 10, 64)
			if err != nil {
				return nil, readerr.Mark(fmt.Errorf("tarstream: bad GNU.sparse.realsize: %w", err), false)
			}
			if realsize < 0 || realsize > 1<<62 {
				return nil, readerr.Mark(fmt.Errorf("tarstream: bad GNU.sparse.realsize %d", realsize), false)
			}
			extents, mapLen, err := readSparseMap(r, realsize)
			if err != nil {
				return nil, err
			}
			if mapLen > stored {
				return nil, readerr.Mark(fmt.Errorf("tarstream: sparse map exceeds stored size"), false)
			}
			packedSize := int64(0)
			for _, extent := range extents {
				if extent.Size > stored-mapLen-packedSize {
					return nil, readerr.Mark(fmt.Errorf("tarstream: sparse extents do not match stored size"), false)
				}
				packedSize += extent.Size
			}
			if packedSize != stored-mapLen {
				return nil, readerr.Mark(fmt.Errorf("tarstream: sparse extents do not match stored size"), false)
			}
			payloadSize, err := parsePayloadSize(payloadValue, hasPayloadSize, realsize)
			if err != nil {
				return nil, err
			}
			if entryPresence != nil {
				if uint64(payloadSize) != presencePayload || !entryCanonicalMeta || !entryPAXHeaderSeen || !canonicalPayloadHeaders(blk[:], entryPAXHeader[:], entryPAX, effName, realsize, stored, payloadSize, true) {
					return nil, ErrInvalidCanonicalTarstream
				}
				if err := validatePresenceExtents(entryPresence, presencePayload, extents); err != nil {
					return nil, err
				}
			}
			var holes []sparse.Extent
			if entryPresence == nil {
				holes = extentsToHoles(realsize, extents)
			}
			if !matched {
				if err := skip(((stored + 511) &^ 511) - mapLen); err != nil {
					return nil, err
				}
				continue
			}
			return &meta{
				name:        effName,
				ordinal:     ordinal - 1,
				logical:     realsize,
				stored:      stored,
				mapLen:      mapLen,
				extents:     extents,
				holes:       holes,
				presence:    entryPresence,
				payloadSize: payloadSize, hasPayloadSize: hasPayloadSize,
				canonicalPayload: entryCanonicalMeta && entryPAXHeaderSeen && (entryPresence != nil || len(holes) > 0) &&
					canonicalPayloadHeaders(blk[:], entryPAXHeader[:], entryPAX, effName, realsize, stored, payloadSize, true),
			}, nil
		}

		payloadSize, err := parsePayloadSize(payloadValue, hasPayloadSize, stored)
		if err != nil {
			return nil, err
		}
		m := &meta{
			name: effName, ordinal: ordinal - 1, logical: stored, stored: stored,
			payloadSize: payloadSize, hasPayloadSize: hasPayloadSize,
			canonicalPayload: entryCanonicalMeta && entryPAXHeaderSeen &&
				canonicalPayloadHeaders(blk[:], entryPAXHeader[:], entryPAX, effName, stored, stored, payloadSize, false),
		}
		if stored > 0 {
			m.extents = []extent{{Offset: 0, Size: stored}}
		}
		return m, nil
	}
}

// canonicalPayloadHeaders recognizes the deterministic transport metadata
// emitted by payloadPrefix. The carrier identity commits to the logical name,
// size, extents, and bytes; this check prevents uncommitted tar ownership or
// permission metadata from changing extraction semantics under that identity.
func canonicalPayloadHeaders(block, paxBlock []byte, paxRecords map[string]string, name string, logical, stored, payloadSize int64, sparseEncoding bool) bool {
	want := makePayloadHeaders(name, logical, stored, payloadSize, sparseEncoding, paxRecords[presencePAX] == "1")
	return maps.Equal(paxRecords, want.records) &&
		bytes.Equal(paxBlock, want.paxHeader[:]) && bytes.Equal(block, want.payloadHeader[:])
}

func parsePayloadSize(raw string, present bool, logical int64) (int64, error) {
	if !present {
		return logical, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || value > logical {
		return 0, readerr.Mark(fmt.Errorf("tarstream: invalid %s %q", payloadSizePAX, raw), false)
	}
	return value, nil
}

// readSparseMap parses the GNU PAX sparse 1.0 map that prefixes the
// stored data: decimal extent count, then offset/size per line,
// consumed in whole 512-byte blocks. Zero-length extents (GNU's
// trailing sentinel) are dropped.
func readSparseMap(r io.Reader, realsize int64) ([]extent, int64, error) {
	var (
		blk    [512]byte
		buf    []byte
		mapLen int64
	)
	next := func() (int64, error) {
		for {
			if i := bytes.IndexByte(buf, '\n'); i >= 0 {
				v, err := strconv.ParseInt(string(buf[:i]), 10, 64)
				if err != nil || v < 0 {
					return 0, readerr.Mark(fmt.Errorf("tarstream: bad sparse map value %q", buf[:i]), false)
				}
				buf = buf[i+1:]
				return v, nil
			}
			if len(buf) > 64 { // a decimal never runs this long
				return 0, readerr.Mark(fmt.Errorf("tarstream: malformed sparse map"), false)
			}
			if _, err := readFixed(r, blk[:]); err != nil {
				return 0, fmt.Errorf("tarstream: sparse map: %w", fixedReadError(err))
			}
			mapLen += 512
			buf = append(buf, blk[:]...)
		}
	}
	count, err := next()
	if err != nil {
		return nil, 0, err
	}
	if count > maxSparseExtents {
		return nil, 0, readerr.Mark(fmt.Errorf("tarstream: absurd sparse extent count %d", count), false)
	}
	var extents []extent
	var pos int64
	for range count {
		off, err := next()
		if err != nil {
			return nil, 0, err
		}
		sz, err := next()
		if err != nil {
			return nil, 0, err
		}
		if sz == 0 {
			continue // sentinel
		}
		if off < pos || off > realsize || sz > realsize-off {
			return nil, 0, readerr.Mark(fmt.Errorf("tarstream: sparse extent [%d,+%d) out of order or bounds", off, sz), false)
		}
		extents = append(extents, extent{Offset: off, Size: sz})
		pos = off + sz
	}
	return extents, mapLen, nil
}

// readPAX reads and parses a PAX extended header's records.
func readPAX(r io.Reader, size, padded int64) (map[string]string, bool, error) {
	if size < 0 || padded < size || size > 1<<20 {
		return nil, false, readerr.Mark(fmt.Errorf("tarstream: absurd PAX header size %d", size), false)
	}
	data := make([]byte, padded)
	if _, err := readFixed(r, data); err != nil {
		return nil, false, err
	}
	paddingZero := isZeroBlock(data[size:])
	raw := data[:size]
	data = raw
	recs := map[string]string{}
	for len(data) > 0 {
		sp := bytes.IndexByte(data, ' ')
		if sp <= 0 {
			return nil, false, readerr.Mark(fmt.Errorf("tarstream: malformed PAX record"), false)
		}
		n, err := strconv.Atoi(string(data[:sp]))
		if err != nil || n <= sp || int64(n) > int64(len(data)) || data[n-1] != '\n' {
			return nil, false, readerr.Mark(fmt.Errorf("tarstream: malformed PAX record length"), false)
		}
		kv := data[sp+1 : n-1]
		eq := bytes.IndexByte(kv, '=')
		if eq < 0 {
			return nil, false, readerr.Mark(fmt.Errorf("tarstream: malformed PAX record (no =)"), false)
		}
		recs[string(kv[:eq])] = string(kv[eq+1:])
		data = data[n:]
	}
	return recs, paddingZero && bytes.Equal(raw, encodePAX(recs)), nil
}

// ustarName joins the ustar prefix and name fields.
func ustarName(blk []byte) string {
	name := strings.TrimRight(string(blk[0:100]), "\x00")
	prefix := strings.TrimRight(string(blk[345:500]), "\x00")
	if prefix != "" {
		return prefix + "/" + name
	}
	return name
}

// parseNumeric decodes a ustar numeric field: NUL/space-terminated
// octal, or GNU base-256 when the high bit of the first byte is set.
func parseNumeric(b []byte) (int64, error) {
	if len(b) > 0 && b[0]&0x80 != 0 {
		var v int64
		v = int64(b[0] & 0x7f)
		for _, c := range b[1:] {
			v = v<<8 | int64(c)
		}
		return v, nil
	}
	s := strings.Trim(string(b), " \x00")
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 8, 64)
}

func isZeroBlock(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// normalizeName strips leading "/" and "./" so stored-name dialects
// compare equal.
func normalizeName(s string) string {
	for {
		switch {
		case strings.HasPrefix(s, "/"):
			s = strings.TrimPrefix(s, "/")
		case strings.HasPrefix(s, "./"):
			s = strings.TrimPrefix(s, "./")
		default:
			return s
		}
	}
}

// seqView is the one-pass logical view over a non-seekable source.
type seqView struct {
	meta
	src        io.Reader // packed data, already bounded to stored-mapLen
	pos        int64
	extIdx     int
	packedRead int64
	finish     func() error
	finished   bool
	finalErr   error
}

func (v *seqView) Name() string           { return v.meta.name }
func (v *seqView) Size() int64            { return v.logical }
func (v *seqView) Holes() []sparse.Extent { return v.meta.logicalHoles() }

func (v *seqView) Read(p []byte) (int, error) {
	if v.finalErr != nil {
		return 0, v.finalErr
	}
	if v.pos >= v.logical {
		return 0, io.EOF
	}
	for v.extIdx < len(v.extents) && v.pos >= v.extents[v.extIdx].Offset+v.extents[v.extIdx].Size {
		v.extIdx++
	}
	// Zero region: before the next extent, or the trailing hole.
	zeroEnd := v.logical
	if v.extIdx < len(v.extents) && v.pos < v.extents[v.extIdx].Offset {
		zeroEnd = v.extents[v.extIdx].Offset
	} else if v.extIdx < len(v.extents) {
		// Inside the current extent: read packed bytes.
		e := v.extents[v.extIdx]
		n := int64(len(p))
		if rest := e.Offset + e.Size - v.pos; rest < n {
			n = rest
		}
		read, err := v.src.Read(p[:n])
		if read > 0 {
			v.pos += int64(read)
			v.packedRead += int64(read)
			if err != nil && err != io.EOF {
				v.finalErr = err
				return read, err
			}
			if v.packedRead == v.stored-v.mapLen {
				if finalErr := v.finishNow(); finalErr != nil {
					return read, finalErr
				}
			}
			return read, nil
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	n := int64(len(p))
	if rest := zeroEnd - v.pos; rest < n {
		n = rest
	}
	clear(p[:n])
	v.pos += n
	return int(n), nil
}

func (v *seqView) finishNow() error {
	if v.finished {
		return v.finalErr
	}
	v.finished = true
	if v.finish != nil {
		v.finalErr = v.finish()
	}
	return v.finalErr
}

// seekView is the random-access logical view over a seekable source.
type seekView struct {
	meta
	rs           io.ReadSeeker
	dataStart    int64   // absolute offset of the packed data region
	packedPrefix []int64 // prefix sums of extent sizes
	pos          int64
}

func (v *seekView) Name() string           { return v.meta.name }
func (v *seekView) Size() int64            { return v.logical }
func (v *seekView) Holes() []sparse.Extent { return v.meta.logicalHoles() }

func (v *seekView) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		base = v.pos
	case io.SeekEnd:
		base = v.logical
	default:
		return 0, readerr.Mark(fmt.Errorf("tarstream: bad whence %d", whence), false)
	}
	n := base + offset
	if n < 0 {
		return 0, readerr.Mark(fmt.Errorf("tarstream: negative seek position"), false)
	}
	v.pos = n
	return n, nil
}

func (v *seekView) Read(p []byte) (int, error) {
	n, err := v.readAtPos(p, v.pos)
	v.pos += int64(n)
	return n, err
}

// readAtPos serves one partial read of the logical view at pos
// without touching v.pos: data extents map onto the packed region,
// everything else reads as zeros.
func (v *seekView) readAtPos(p []byte, pos int64) (int, error) {
	if pos >= v.logical {
		return 0, io.EOF
	}
	// Find the extent at or after pos.
	idx := 0
	for idx < len(v.extents) && pos >= v.extents[idx].Offset+v.extents[idx].Size {
		idx++
	}
	if idx < len(v.extents) && pos >= v.extents[idx].Offset {
		// Data: map the logical position into the packed region.
		e := v.extents[idx]
		packed := v.packedPrefix[idx] + (pos - e.Offset)
		n := int64(len(p))
		if rest := e.Offset + e.Size - pos; rest < n {
			n = rest
		}
		if _, err := v.rs.Seek(v.dataStart+packed, io.SeekStart); err != nil {
			return 0, err
		}
		read, err := readFixed(v.rs, p[:n])
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return read, err
	}
	// Hole (or trailing hole): zeros until the next extent or EOF.
	zeroEnd := v.logical
	if idx < len(v.extents) {
		zeroEnd = v.extents[idx].Offset
	}
	n := int64(len(p))
	if rest := zeroEnd - pos; rest < n {
		n = rest
	}
	clear(p[:n])
	return int(n), nil
}

// sourceView adapts a located entry to sparse.Source. A separate type
// because Reader.Size returns int64 while sparse.Source.Size returns
// uint64 — one type cannot carry both methods.
type sourceView struct {
	m   *meta
	seq *seqView
}

func (s *sourceView) Size() uint64 { return uint64(s.m.logical) }

// PayloadCommitment exposes the declared payload boundary before sequential
// consumption. Its digest becomes trustworthy only at the full-read boundary.
func (s *sourceView) PayloadCommitment() (uint64, [32]byte, bool) {
	if s.m.hasPayloadSize && s.m.payloadSize >= 0 {
		return uint64(s.m.payloadSize), [32]byte{}, false
	}
	return s.Size(), [32]byte{}, false
}
func (s *sourceView) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }

// RunAt classifies offsets from the envelope's map: Data inside a
// stored extent, Hole everywhere else. Zero is never produced — the
// envelope has no such state.
func (s *sourceView) RunAt(offset, limit uint64) (sparse.Run, error) {
	kind, end, err := s.m.runAt(offset, limit)
	if err != nil {
		return nil, err
	}
	return tarSourceRun[*sourceView]{source: s, offset: offset, end: end, kind: kind}, nil
}

// runAt classifies one run of the entry's logical view from its
// extent map: Data inside a stored extent, Hole everywhere else.
func (m *meta) runAt(offset, limit uint64) (sparse.RunKind, uint64, error) {
	size := uint64(m.logical)
	if offset >= size {
		return 0, 0, io.EOF
	}
	if limit == 0 {
		return 0, 0, readerr.Mark(fmt.Errorf("tarstream: invalid run: zero limit at offset %d", offset), false)
	}
	limEnd := offset + limit
	if limEnd < offset || limEnd > size {
		limEnd = size
	}
	idx := m.extentAt(offset)
	kind := sparse.Hole
	end := limEnd
	if idx < len(m.extents) {
		e := m.extents[idx]
		if offset < uint64(e.Offset) {
			end = min(end, uint64(e.Offset))
		} else {
			kind = sparse.Data
			end = min(end, uint64(e.Offset+e.Size))
		}
	}
	if m.presence != nil && offset < uint64(m.payloadSize) {
		pages := min(presencePages(uint64(m.payloadSize)), presencePages(end))
		page := offset / presencePage
		end = min(end, presenceRunEnd(m.presence, page, pages)*presencePage)
		if !presenceBit(m.presence, page) {
			kind = sparse.Hole
		} else if kind != sparse.Data {
			kind = sparse.Zero
		}
	}
	return kind, end, nil
}

// extentAt returns the index of the first extent that ends past
// offset (== len(extents) when offset is in the trailing hole).
func (m *meta) extentAt(offset uint64) int {
	lo, hi := 0, len(m.extents)
	for lo < hi {
		mid := (lo + hi) / 2
		if uint64(m.extents[mid].Offset+m.extents[mid].Size) <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// readerAtSource is the concurrent random-access sparse.Source over an
// io.ReaderAt (SourceAt). It holds no mutable state: every ReadAt is
// offset arithmetic plus ra.ReadAt, so concurrency is inherited from
// ra.
type readerAtSource struct {
	meta
	ra           io.ReaderAt
	dataStart    int64   // absolute offset of the packed data region
	packedPrefix []int64 // prefix sums of extent sizes
}

func (s *readerAtSource) Size() uint64 { return uint64(s.logical) }

func (s *readerAtSource) RunAt(offset, limit uint64) (sparse.Run, error) {
	kind, end, err := s.meta.runAt(offset, limit)
	if err != nil {
		return nil, err
	}
	return tarSourceRun[*readerAtSource]{source: s, offset: offset, end: end, kind: kind}, nil
}

// tarSourceRun preserves the owning source's one-pass or concurrent-access
// guarantee while enforcing the exact, in-run ReadAt contract.
type tarRunReader interface {
	ReadAt(context.Context, []byte, uint64) (int, error)
}

type tarSourceRun[S tarRunReader] struct {
	source S
	offset uint64
	end    uint64
	kind   sparse.RunKind
}

func (r tarSourceRun[S]) Offset() uint64       { return r.offset }
func (r tarSourceRun[S]) End() uint64          { return r.end }
func (r tarSourceRun[S]) Kind() sparse.RunKind { return r.kind }

func (r tarSourceRun[S]) ReadAt(ctx context.Context, buf []byte, innerOffset uint64) (int, error) {
	if r.end <= r.offset {
		return 0, readerr.Mark(fmt.Errorf("tarstream: invalid run [%d,%d)", r.offset, r.end), false)
	}
	runLength := r.end - r.offset
	if innerOffset > runLength || uint64(len(buf)) > runLength-innerOffset {
		return 0, readerr.Mark(fmt.Errorf("tarstream: run read offset %d length %d outside [0,%d)", innerOffset, len(buf), runLength), false)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.kind == sparse.Hole || r.kind == sparse.Zero {
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
	return n, readerr.Mark(fmt.Errorf("tarstream: short run read @ %d: %d of %d bytes", r.offset+innerOffset, n, len(buf)), false)
}

func (s *readerAtSource) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	size := uint64(s.logical)
	if offset >= size {
		return 0, io.EOF
	}
	n := len(buf)
	var eof error
	if uint64(n) > size-offset {
		n = int(size - offset)
		eof = io.EOF
	}
	p := buf[:n]
	pos := int64(offset)
	for len(p) > 0 {
		idx := s.extentAt(uint64(pos))
		if idx < len(s.extents) && pos >= s.extents[idx].Offset {
			// Data: map the logical position into the packed region.
			e := s.extents[idx]
			packed := s.packedPrefix[idx] + (pos - e.Offset)
			m := int64(len(p))
			if rest := e.Offset + e.Size - pos; rest < m {
				m = rest
			}
			if err := readAtFull(s.ra, p[:m], s.dataStart+packed); err != nil {
				return 0, fmt.Errorf("tarstream: packed read @ %d: %w", pos, err)
			}
			p = p[m:]
			pos += m
			continue
		}
		// Hole (or trailing hole): zeros until the next extent or EOF.
		zeroEnd := s.logical
		if idx < len(s.extents) {
			zeroEnd = s.extents[idx].Offset
		}
		m := int64(len(p))
		if rest := zeroEnd - pos; rest < m {
			m = rest
		}
		clear(p[:m])
		p = p[m:]
		pos += m
	}
	return n, eof
}

func (s *sourceView) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	size := uint64(s.m.logical)
	if offset >= size {
		return 0, io.EOF
	}
	n := len(buf)
	var eof error
	if uint64(n) > size-offset {
		n = int(size - offset)
		eof = io.EOF
	}
	// One-pass: discard forward, then fill. The view synthesizes
	// zeros across holes, so skipping a hole region never touches the
	// underlying source.
	pos := uint64(s.seq.pos)
	if offset < pos {
		return 0, readerr.Mark(fmt.Errorf("tarstream: backward ReadAt @ %d (position %d) on one-pass source", offset, pos), false)
	}
	if offset > pos {
		if _, err := io.CopyN(io.Discard, s.seq, int64(offset-pos)); err != nil {
			return 0, err
		}
		if s.seq.finalErr != nil {
			return 0, s.seq.finalErr
		}
	}
	read, err := readFixed(s.seq, buf[:n])
	if err != nil {
		return read, err
	}
	if s.seq.finalErr != nil {
		return read, s.seq.finalErr
	}
	return n, eof
}

// artifactReaderAt enforces one complete random-access attempt before the
// sequential metadata parser sees it. io.ReadFull must not consume a full
// buffer accompanied by a terminal source error, nor stitch short attempts.
type artifactReaderAt struct{ io.ReaderAt }

func (r artifactReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if err := readAtFull(r.ReaderAt, p, off); err != nil {
		return 0, err
	}
	return len(p), nil
}

// A declared fixed-size field ending early is malformed. Wrapped transport
// errors keep their own retry classification, including internal cancellation.
func fixedReadError(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return readerr.Mark(err, false)
	}
	return err
}

// readFixed consumes one sequential field, preserving even a full-length
// source failure. Partial successful Reader reads remain ordinary streaming.
func readFixed(r io.Reader, p []byte) (n int, err error) {
	for n < len(p) {
		m, e := r.Read(p[n:])
		if m < 0 || m > len(p)-n {
			return n, readerr.Mark(fmt.Errorf("tarstream: invalid Reader count %d", m), false)
		}
		n += m
		if e != nil {
			if e == io.EOF {
				if n == len(p) {
					return n, nil
				}
				if n > 0 {
					e = io.ErrUnexpectedEOF
				}
			}
			if n == len(p) && e == io.ErrUnexpectedEOF {
				e = readerr.Mark(e, false)
			}
			return n, e
		}
	}
	return n, nil
}
