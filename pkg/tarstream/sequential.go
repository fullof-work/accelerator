package tarstream

import (
	"bytes"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type trackedPlainReader struct {
	r    io.Reader
	read int64
}

func newTrackedPlainReader(r io.Reader) *trackedPlainReader {
	return &trackedPlainReader{r: r}
}

func (r *trackedPlainReader) Read(dst []byte) (int, error) {
	n, err := r.r.Read(dst)
	if n > 0 {
		r.read += int64(n)
	}
	return n, err
}

type sequentialVerifier struct {
	reader    *trackedPlainReader
	meta      *meta
	options   readOptions
	identity  *identityAccumulator
	encrypted bool
	done      bool
	err       error
}

func (v *sequentialVerifier) finish() error {
	if v.done {
		return v.err
	}
	v.done = true
	pad := int((512 - v.meta.stored%512) % 512)
	if pad > 0 {
		padding := make([]byte, pad)
		if _, err := readFixed(v.reader, padding); err != nil {
			v.err = &canonicalReadError{category: fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream), cause: fixedReadError(err)}
			return v.err
		}
		if !isZeroBlock(padding) {
			v.err = fmt.Errorf("%w: invalid payload padding", ErrInvalidCanonicalTarstream)
			return v.err
		}
	}
	var marker [512]byte
	if _, err := readFixed(v.reader, marker[:]); err != nil {
		v.err = &canonicalReadError{category: fmt.Errorf("%w: missing digest marker", ErrInvalidCanonicalTarstream), cause: fixedReadError(err)}
		return v.err
	}
	var markerBody [512]byte
	if _, err := readFixed(v.reader, markerBody[:]); err != nil {
		v.err = &canonicalReadError{category: fmt.Errorf("%w: invalid digest marker body", ErrInvalidCanonicalTarstream), cause: fixedReadError(err)}
		return v.err
	}
	declared, err := parseCanonicalMarker(marker[:], markerBody[:])
	if err != nil {
		v.err = err
		return v.err
	}
	var trailer [1024]byte
	if _, err := readFixed(v.reader, trailer[:]); err != nil {
		v.err = &canonicalReadError{category: fmt.Errorf("%w: invalid end-of-archive", ErrInvalidCanonicalTarstream), cause: fixedReadError(err)}
		return v.err
	}
	if !isZeroBlock(trailer[:512]) || !isZeroBlock(trailer[512:]) {
		v.err = fmt.Errorf("%w: invalid end-of-archive", ErrInvalidCanonicalTarstream)
		return v.err
	}
	var extra [1]byte
	n, err := readOneByte(v.reader, extra[:])
	if n != 0 {
		v.err = fmt.Errorf("%w: trailing artifact bytes", ErrInvalidCanonicalTarstream)
		return v.err
	}
	if err != io.EOF {
		if err != nil {
			v.err = err
		} else {
			v.err = fmt.Errorf("%w: trailing artifact bytes", ErrInvalidCanonicalTarstream)
		}
		return v.err
	}
	if !v.meta.hasPayloadSize || v.meta.payloadSize < 0 || uint64(v.meta.payloadSize) != declared.payloadSize {
		v.err = ErrDigestMismatch
		return v.err
	}
	if !v.meta.canonicalPayload {
		v.err = fmt.Errorf("%w: non-canonical payload header metadata", ErrInvalidCanonicalTarstream)
		return v.err
	}
	recomputed := v.identity.finish(v.meta.name, uint64(v.meta.logical), uint64(v.meta.payloadSize))
	if subtle.ConstantTimeCompare(recomputed.payload[:], declared.payload[:]) != 1 ||
		subtle.ConstantTimeCompare(recomputed.digest[:], declared.digest[:]) != 1 {
		v.err = ErrDigestMismatch
		return v.err
	}
	var codec Codec
	if v.encrypted {
		codec = v.options.codec
	}
	scheme, external := externalDigest(codec, recomputed.digest)
	if err := checkExpected(v.options, scheme, external); err != nil {
		v.err = err
	}
	return v.err
}

func parseCanonicalMarker(block, body []byte) (carrierIdentity, error) {
	var zero carrierIdentity
	if len(block) != 512 || string(block[257:262]) != "ustar" || !validHeaderChecksum(block) {
		return zero, fmt.Errorf("%w: invalid digest marker header", ErrInvalidCanonicalTarstream)
	}
	typeflag := block[156]
	if typeflag != 0 && typeflag != '0' {
		return zero, fmt.Errorf("%w: digest marker is not a regular file", ErrInvalidCanonicalTarstream)
	}
	size, err := parseNumeric(block[124:136])
	if err != nil || size != 40 || len(body) != 512 {
		return zero, fmt.Errorf("%w: invalid digest marker body", ErrInvalidCanonicalTarstream)
	}
	digest, ok := parseDigestMarker(normalizeName(ustarName(block)))
	if !ok {
		return zero, fmt.Errorf("%w: invalid digest marker", ErrInvalidCanonicalTarstream)
	}
	if !isZeroBlock(body[40:]) {
		return zero, fmt.Errorf("%w: nonzero digest marker padding", ErrInvalidCanonicalTarstream)
	}
	identity := carrierIdentity{digest: digest, payloadSize: binary.BigEndian.Uint64(body[:8])}
	copy(identity.payload[:], body[8:40])
	return identity, nil
}

func validHeaderChecksum(block []byte) bool {
	want, err := parseNumeric(block[148:156])
	if err != nil {
		return false
	}
	var unsigned, signed int64
	for i, value := range block {
		if i >= 148 && i < 156 {
			value = ' '
		}
		unsigned += int64(value)
		signed += int64(int8(value))
	}
	return want == unsigned || want == signed
}

const maxConsecutiveEmptyReads = 100

func readOneByte(reader io.Reader, buffer []byte) (int, error) {
	for range maxConsecutiveEmptyReads {
		n, err := reader.Read(buffer)
		if n != 0 || err != nil {
			return n, err
		}
	}
	return 0, io.ErrNoProgress
}

func openSequentialPlaintext(r io.Reader, options readOptions) (io.Reader, *envelopeHeader, error) {
	first := make([]byte, len(envelopeMagic))
	n, err := readFixed(r, first)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if options.required {
				return nil, nil, ErrPlaintextForbidden
			}
			return io.MultiReader(bytes.NewReader(first[:n]), r), nil, nil
		}
		return nil, nil, err
	}
	if !bytes.Equal(first, envelopeMagic[:]) {
		if options.required {
			return nil, nil, ErrPlaintextForbidden
		}
		return io.MultiReader(bytes.NewReader(first), r), nil, nil
	}
	if options.codec == nil {
		return nil, nil, ErrCodecRequired
	}
	var prefix [envelopePrefixSize]byte
	copy(prefix[:8], first)
	if _, err := readFixed(r, prefix[8:]); err != nil {
		return nil, nil, &canonicalReadError{category: fmt.Errorf("%w: truncated clear prefix", ErrMalformedEnvelope), cause: fixedReadError(err)}
	}
	if err := validatePrefix(prefix); err != nil {
		return nil, nil, err
	}
	recordCodec, err := bindRecordCodec(options.codec, prefixSalt(prefix))
	if err != nil {
		return nil, nil, err
	}
	if recordCodec.CiphertextSize(envelopeHeaderSize) != envelopeHeaderSealed {
		return nil, nil, fmt.Errorf("%w: codec header size", ErrMalformedEnvelope)
	}
	sealed := make([]byte, envelopeHeaderSealed)
	if _, err := readFixed(r, sealed); err != nil {
		return nil, nil, &canonicalReadError{category: fmt.Errorf("%w: truncated encrypted header", ErrMalformedEnvelope), cause: fixedReadError(err)}
	}
	plaintext, err := recordCodec.DecryptInPlace(sealed, headerAAD(prefix), 0)
	if err != nil {
		return nil, nil, &canonicalReadError{category: fmt.Errorf("%w: encrypted header", ErrAuthentication), cause: err}
	}
	if len(plaintext) != envelopeHeaderSize || &plaintext[0] != &sealed[1] {
		return nil, nil, fmt.Errorf("%w: codec violated in-place header contract", ErrMalformedEnvelope)
	}
	var plainHeader [envelopeHeaderSize]byte
	copy(plainHeader[:], plaintext)
	geometry, err := parseEnvelopeHeader(plainHeader[:])
	if err != nil {
		return nil, nil, err
	}
	if err := validateCodecGeometry(recordCodec, geometry); err != nil {
		return nil, nil, err
	}
	return newRecordSeqReader(r, recordCodec, prefix, plainHeader, geometry), &geometry, nil
}

func sourceFromSequential(r io.Reader, name string, options readOptions) (sparse.Source, string, error) {
	plaintext, geometry, err := openSequentialPlaintext(r, options)
	if err != nil {
		return nil, "", err
	}
	tracked := newTrackedPlainReader(plaintext)
	view, err := newSeqView(tracked, name)
	if err != nil {
		return nil, "", keyBoundCanonicalError(options, err)
	}
	if view.meta.ordinal != 0 && !(view.meta.ordinal == 1 && view.meta.presence != nil) {
		return nil, "", keyBoundCanonicalError(options, fmt.Errorf("%w: payload must be the first archive entry", ErrInvalidCanonicalTarstream))
	}
	if geometry != nil {
		packedSize := view.meta.stored - view.meta.mapLen
		if tracked.read < 0 || packedSize < 0 || uint64(tracked.read) != geometry.packedStart || uint64(packedSize) != geometry.packedSize {
			return nil, "", fmt.Errorf("%w: inner tar layout does not match authenticated header", ErrMalformedEnvelope)
		}
	}
	payloadExtents, payloadPacked, tailDense := prefixExtents(view.meta.extents, view.meta.payloadSize, view.meta.logical)
	if !view.meta.hasPayloadSize || !tailDense {
		return nil, "", fmt.Errorf("%w: invalid payload boundary", ErrInvalidCanonicalTarstream)
	}
	if tailSize := view.meta.logical - view.meta.payloadSize; tailSize > maxMetadataTailSize {
		return nil, "", fmt.Errorf("%w: metadata tail size %d exceeds %d", ErrInvalidCanonicalTarstream, tailSize, maxMetadataTailSize)
	}
	identity := newIdentityAccumulator(
		uint64(view.meta.payloadSize), payloadExtents, payloadPacked, view.meta.logical-view.meta.payloadSize, view.meta.presence, view.meta.extents,
	)
	view.src = io.TeeReader(view.src, identity)
	verifier := &sequentialVerifier{reader: tracked, meta: &view.meta, options: options, identity: identity, encrypted: geometry != nil}
	view.finish = verifier.finish
	if view.meta.stored-view.meta.mapLen == 0 {
		if err := view.finishNow(); err != nil {
			return nil, "", err
		}
	}
	return &sourceView{m: &view.meta, seq: view}, view.meta.name, nil
}
