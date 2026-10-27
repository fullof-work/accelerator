// Package tarstream packages one sparse-capable byte source as a tar artifact
// and reads it back without materializing anything. WriteTo emits a full
// presence bitmap followed by a sparse.Source's logical view as a GNU PAX
// sparse 1.0 entry (including its trailing (size,0) sentinel extent), then appends a
// .kuasar.digest.<hex> marker naming the carrier-provided logical identity.
// The marker also retains the payload commitment needed to derive a new
// identity after replacing an E/S metadata tail. ReadFrom / ReadSeekFrom return
// the logical view with the exact
// hole map; SourceFrom opens it as a sparse.Source for pipelines. A size-bounded
// SourceAt exposes the marker through the optional Digester capability without
// rereading payload bytes. WithCodec places the complete canonical stream,
// including marker and trailer, in fixed 4 KiB authenticated encrypted-v1
// records; SourceAt decrypts records lazily and SourceFrom performs full inner
// digest/trailer/outer-EOF validation when its Data extents are consumed. Only
// data bytes flow on the inner tar wire — holes cost nothing.
//
// All new writes use presence v1. The logical payload may be empty or end in a
// partial 4 KiB page, and is limited to 1 TiB. Hole runs must start on a page
// boundary and end on a page boundary or at the exact payload end. An optional
// declared dense metadata tail has its own limit. Invalid layouts fail before
// output or Data reads; the writer never falls back to the old encoding or
// changes logical boundaries. Whole Zero pages remain logically present while
// their bytes are omitted from the GNU sparse map; partial-page Zero is stored
// as literal Data. Read-side Sources reconstruct Hole, Zero and Data without
// scanning bytes. Old portable carriers remain readable, including empty
// payloads and exact byte-hole boundaries. Rewriting them requires a logical
// source that meets the new writer constraints.
//
// The tar envelope keeps the format inspectable and interoperable:
// GNU tar extracts the stream back into a sparse file, Go's
// archive/tar reads the logical bytes, and this package round-trips
// the hole map. The reverse holds too: ReadFrom understands archives
// produced by `tar --format=posix --sparse` (the modern GNU sparse
// encoding); the legacy GNU binary sparse format and PAX 0.x variants
// are rejected with ErrUnsupportedEncoding.
//
// The package lives at the dependency root and depends only on the
// stdlib plus pkg/sparse.
package tarstream

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// DigestMarkerPrefix is the reserved name prefix of the metadata entry that
// terminates a Kuasar tarstream artifact. The suffix is exactly 64 lowercase
// hexadecimal characters.
const DigestMarkerPrefix = ".kuasar.digest."

const (
	payloadSizePAX = "KUASAR.payload.size"
	// Identity reuse is intentionally O(metadata tail), with a fixed ceiling
	// above the maximum canonical E/S metadata carried today.
	maxMetadataTailSize = 64 << 20
)

// Digester is an optional capability implemented by sources opened from a
// complete Kuasar artifact. Digest performs no I/O and does not recompute
// payload bytes. Digest never includes its scheme prefix.
type Digester interface {
	Digest() (scheme string, digest string)
}

// IdentityProvider is implemented by a complete carrier or a deterministic
// logical source that can provide its tarstream identity without reading the
// payload. The returned digest is the plaintext canonical identity; writers
// apply the configured keyed mapping before exposing it in a ref.
type IdentityProvider interface {
	TarStreamDigest(name string) (digest [32]byte, ok bool)
	// PayloadCommitment always returns the logical boundary before an optional dense
	// metadata tail. ok reports whether digest contains a carrier-declared
	// commitment that can be reused without reading that payload. A full carrier
	// read verifies the declaration against the payload bytes.
	PayloadCommitment() (size uint64, digest [32]byte, ok bool)
}

// CarrierDigest returns the public identity the tarstream carrier will emit
// for src under the supplied write policy. It performs no payload I/O: src
// must be an opened carrier or a deterministic carrier-derived source that
// implements IdentityProvider.
func CarrierDigest(name string, src sparse.Source, options ...WriteOption) (string, string, error) {
	opts, err := parseWriteOptions(options)
	if err != nil {
		return "", "", err
	}
	if opts.beforeCommit != nil {
		return "", "", fmt.Errorf("%w: WithBeforeCommit requires WriteTo", ErrInvalidOption)
	}
	provider, ok := src.(IdentityProvider)
	if !ok {
		return "", "", fmt.Errorf("tarstream: source carrier does not provide a digest")
	}
	name = normalizeName(name)
	plain, ok := provider.TarStreamDigest(name)
	if !ok {
		return "", "", fmt.Errorf("tarstream: source carrier cannot derive digest for %q", name)
	}
	scheme, digest := externalDigest(opts.codec, plain)
	return scheme, digestHex(digest), nil
}

// ComposeDigest derives the plaintext carrier identity from a previously
// authenticated payload commitment and a replacement dense tail.
func ComposeDigest(name string, totalSize, payloadSize uint64, payloadCommitment [32]byte, tail []byte) ([32]byte, error) {
	if payloadSize > totalSize || uint64(len(tail)) != totalSize-payloadSize || uint64(len(tail)) > maxMetadataTailSize {
		return [32]byte{}, ErrInvalidDigest
	}
	tailHash := newTailHasher(uint64(len(tail)))
	_, _ = tailHash.Write(tail)
	var tailDigest [32]byte
	copy(tailDigest[:], tailHash.Sum(nil))
	return composeDigest(normalizeName(name), totalSize, payloadSize, payloadCommitment, tailDigest), nil
}

type carrierIdentity struct {
	digest      [32]byte
	payload     [32]byte
	payloadSize uint64
}

func parseDigestMarker(name string) ([32]byte, bool) {
	var digest [32]byte
	if !strings.HasPrefix(name, DigestMarkerPrefix) {
		return digest, false
	}
	hexDigest := strings.TrimPrefix(name, DigestMarkerPrefix)
	if len(hexDigest) != 64 || strings.ToLower(hexDigest) != hexDigest {
		return digest, false
	}
	if _, err := hex.Decode(digest[:], []byte(hexDigest)); err != nil {
		return digest, false
	}
	return digest, true
}

func digestHex(digest [32]byte) string { return hex.EncodeToString(digest[:]) }

// Reader is the sequential logical view of the file inside a tar
// stream: Read yields the logical bytes, holes reading as zeros.
// Metadata is available as soon as the constructor returns (the
// sparse map precedes the data on the wire).
type Reader interface {
	io.Reader
	// Name is the entry name inside the tar stream.
	Name() string
	// Size is the logical file size.
	Size() int64
	// Holes returns the canonical hole map (sorted, merged; nil for a
	// dense file). The slice is a copy the caller may keep or modify.
	Holes() []sparse.Extent
}

// ReadSeeker adds random access to Reader: Seek positions within the
// logical file, mapping straight onto the packed data region of the
// underlying tar stream — no extraction, no copies.
type ReadSeeker interface {
	Reader
	io.Seeker
}

// ErrNotFound reports that the named entry (or, for an empty name, any
// regular file entry) is not present in the stream.
var ErrNotFound = readerr.Mark(errors.New("tarstream: entry not found"), false)

// ErrUnsupportedEncoding reports a sparse member in the legacy GNU
// binary format or a PAX 0.x map, which this package does not decode;
// re-create the archive with `tar --format=posix --sparse` or WriteTo.
var ErrUnsupportedEncoding = readerr.Mark(errors.New("tarstream: unsupported sparse encoding (use GNU posix sparse 1.0)"), false)

// extent is one data run of the logical file: bytes
// [Offset, Offset+Size) hold data.
type extent struct {
	Offset, Size int64
}

// extentsToHoles inverts data extents (sorted, non-overlapping) back
// into the canonical hole map over [0, size).
func extentsToHoles(size int64, extents []extent) []sparse.Extent {
	var holes []sparse.Extent
	var pos int64
	for _, e := range extents {
		if e.Offset > pos {
			holes = append(holes, sparse.Extent{Offset: uint64(pos), Size: uint64(e.Offset - pos)})
		}
		pos = e.Offset + e.Size
	}
	if pos < size {
		holes = append(holes, sparse.Extent{Offset: uint64(pos), Size: uint64(size - pos)})
	}
	return holes
}
