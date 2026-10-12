package flatten

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// LayerInput identifies a complete layer before its body is opened. Open must
// return a fresh body at offset zero on every call, for this exact identity.
// The flatten operation owns every returned body (including on failure), calls
// Close exactly once and checks its error. Close must unblock a concurrent Read.
// A one-shot reader is not a LayerInput; spool non-reopenable archives once.
type LayerInput struct {
	MediaType string
	Digest    string // complete compressed blob, sha256:<64 lowercase hex digits>
	Size      int64  // complete compressed blob size
	DiffID    string // complete decoded stream, including tar padding
	Open      func(context.Context) (io.ReadCloser, error)
}

// LayerCompletion is an optional body extension for a pending cache write.
// After the applier/decoder have stopped and Close has returned, flatten calls
// CompleteLayer exactly once. verified is true only when the complete decoded
// and compressed streams, size, digests, trailers and all Close calls succeeded.
// A false result must discard an incomplete write. Implementations must not
// publish a cache hit on Read EOF or Close alone. CompleteLayer may return an
// error, in which case preparation fails before any artifact is emitted.
type LayerCompletion interface {
	CompleteLayer(verified bool) error
}

// LayerLimits bound decoding and the layer applier's in-memory path bookkeeping.
// Zero fields select defaults. Limits are per layer; layers are applied serially.
type LayerLimits struct {
	DecodedBytes  int64
	DecoderBytes  int64
	MetadataBytes int64
}

func (l LayerLimits) normalized() (LayerLimits, error) {
	if l.DecodedBytes == 0 {
		l.DecodedBytes = 128 << 30
	}
	if l.DecoderBytes == 0 {
		l.DecoderBytes = 64 << 20
	}
	if l.MetadataBytes == 0 {
		l.MetadataBytes = 128 << 20
	}
	if l.DecodedBytes < 1 || l.DecoderBytes < 1<<20 || l.MetadataBytes < 1 {
		return l, errors.New("flatten: invalid layer limits")
	}
	return l, nil
}

func sha256Identity(s string) bool {
	if len(s) != len("sha256:")+64 || s[:7] != "sha256:" {
		return false
	}
	for _, c := range s[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (in LayerInput) validate() error {
	if in.Open == nil || in.Size < 0 || !sha256Identity(in.Digest) || !sha256Identity(in.DiffID) {
		return errors.New("flatten: incomplete layer identity")
	}
	switch in.MediaType {
	case "application/vnd.oci.image.layer.v1.tar",
		"application/vnd.docker.image.rootfs.diff.tar",
		"application/vnd.oci.image.layer.nondistributable.v1.tar",
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
		"application/vnd.oci.image.layer.v1.tar+zstd",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+zstd":
		return nil
	default:
		return fmt.Errorf("flatten: unsupported layer media type %q", in.MediaType)
	}
}

// checkedReader stops at its budget plus one sentinel byte. It never drains an
// unbounded body after an error, and verifies bytes actually supplied by Read.
type checkedReader struct {
	ctx      context.Context
	r        io.Reader
	h        hash.Hash
	skipHash bool // plain layers reuse the compressed stream hash
	n, limit int64
	label    string
	err      error
}

func (r *checkedReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	left := r.limit - r.n
	if left < 0 {
		return 0, fmt.Errorf("flatten: %s byte limit exceeded", r.label)
	}
	if left < int64(len(p)) {
		p = p[:int(left)+1]
	}
	n, err := r.r.Read(p)
	if n > 0 {
		if !r.skipHash {
			r.h.Write(p[:n])
		}
		if int64(n) > left {
			r.err = fmt.Errorf("flatten: %s byte limit exceeded", r.label)
			return n, r.err
		}
		r.n += int64(n)
	}
	if cancel := r.ctx.Err(); cancel != nil {
		return n, cancel
	}
	r.err = err
	return n, err
}

func (r *checkedReader) verify(want string) error {
	got := "sha256:" + hex.EncodeToString(r.h.Sum(nil))
	if got != want {
		return fmt.Errorf("flatten: %s digest mismatch: got %s, want %s", r.label, got, want)
	}
	return nil
}

type layerStream struct {
	in                  LayerInput
	body                io.ReadCloser
	compressed, decoded *checkedReader
	decoderClose        func() error
	closeOnce           sync.Once
	cleanupOnce         sync.Once
	closeErr            error
	cleanupErr          error
	stopCancel          func() bool
	cancelDone          chan struct{}
	verified            bool
}

func openLayer(ctx context.Context, in LayerInput, limits LayerLimits) (_ *layerStream, err error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	limits, err = limits.normalized()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := in.Open(ctx)
	if err != nil {
		if body != nil {
			err = errors.Join(err, body.Close())
			if completion, ok := body.(LayerCompletion); ok {
				err = errors.Join(err, completion.CompleteLayer(false))
			}
		}
		return nil, err
	}
	if body == nil {
		return nil, errors.New("flatten: layer opener returned a nil body")
	}
	s := &layerStream{in: in, body: body, cancelDone: make(chan struct{})}
	s.stopCancel = context.AfterFunc(ctx, func() { s.closeBody(); close(s.cancelDone) })
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	s.compressed = &checkedReader{ctx: ctx, r: body, h: sha256.New(), limit: in.Size, label: "compressed layer"}
	var decoder io.Reader = s.compressed
	switch in.MediaType {
	case "application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip", "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip":
		gz, e := gzip.NewReader(s.compressed)
		if e != nil {
			return nil, fmt.Errorf("flatten: gzip header: %w", e)
		}
		decoder, s.decoderClose = gz, gz.Close
	case "application/vnd.oci.image.layer.v1.tar+zstd", "application/vnd.oci.image.layer.nondistributable.v1.tar+zstd":
		z, e := zstd.NewReader(s.compressed, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(limits.DecoderBytes)))
		if e != nil {
			return nil, fmt.Errorf("flatten: zstd header: %w", e)
		}
		decoder, s.decoderClose = z, func() error { z.Close(); return nil }
	}
	s.decoded = &checkedReader{ctx: ctx, r: decoder, h: sha256.New(), limit: limits.DecodedBytes, label: "decoded layer"}
	if decoder == s.compressed {
		// With an explicit plain-tar media type both identities cover exactly
		// the same bytes. Retain independent limits, counts and identity checks
		// while computing their SHA-256 once. Encoded layers keep two hashes.
		s.decoded.h, s.decoded.skipHash = s.compressed.h, true
	}
	return s, nil
}

func (s *layerStream) Read(p []byte) (int, error) { return s.decoded.Read(p) }

func (s *layerStream) closeBody() { s.closeOnce.Do(func() { s.closeErr = s.body.Close() }) }

// Close stops cancellation delivery and waits for any in-flight Close. It does
// not drain; the caller must finish applying the layer before cleaning rootfs.
func (s *layerStream) Close() error {
	s.cleanupOnce.Do(func() {
		if !s.stopCancel() {
			<-s.cancelDone
		}
		s.closeBody()
		s.cleanupErr = errors.Join(s.closeErr, s.closeDecoder(), s.compressed.ctx.Err())
		if completion, ok := s.body.(LayerCompletion); ok {
			s.cleanupErr = errors.Join(s.cleanupErr, completion.CompleteLayer(s.verified && s.cleanupErr == nil))
		}
	})
	return s.cleanupErr
}

// finish is required even after tar EOF. Decoder EOF validates gzip/zstd trailers
// and concatenated members; compressed EOF validates the exact descriptor size.
// Both decoders and the owned body are closed before the layer can be accepted.
func (s *layerStream) finish() (err error) {
	defer func() { err = errors.Join(err, s.Close()) }()
	if _, err = io.Copy(io.Discard, s); err != nil {
		return err
	}
	if _, err = io.Copy(io.Discard, s.compressed); err != nil {
		return err
	}
	if s.compressed.n != s.in.Size {
		return fmt.Errorf("flatten: compressed layer size %d, want %d", s.compressed.n, s.in.Size)
	}
	if err = s.compressed.verify(s.in.Digest); err != nil {
		return err
	}
	if err = s.decoded.verify(s.in.DiffID); err != nil {
		return err
	}
	s.verified = true
	return nil
}

func (s *layerStream) closeDecoder() error {
	if s.decoderClose == nil {
		return nil
	}
	f := s.decoderClose
	s.decoderClose = nil
	return f()
}
