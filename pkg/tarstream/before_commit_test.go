package tarstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// commitSource rejects any data request after its declared end, including an
// extra EOF read. Hole and Zero variants reject every payload read.
type commitSource struct {
	size uint64
	kind sparse.RunKind
	read uint64
	err  error
}

func (s *commitSource) Size() uint64 { return s.size }
func (s *commitSource) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	return commitRun{s, off, off + min(limit, s.size-off)}, nil
}
func (s *commitSource) ReadAt(_ context.Context, p []byte, off uint64) (int, error) {
	if s.kind != sparse.Data || off != s.read || off >= s.size || uint64(len(p)) > s.size-off {
		return 0, errors.New("unexpected payload read")
	}
	if s.err != nil {
		return 0, s.err
	}
	for i := range p {
		p[i] = 'a'
	}
	s.read += uint64(len(p))
	return len(p), nil
}

type commitRun struct {
	source   *commitSource
	off, end uint64
}

func (r commitRun) Offset() uint64       { return r.off }
func (r commitRun) End() uint64          { return r.end }
func (r commitRun) Kind() sparse.RunKind { return r.source.kind }
func (r commitRun) ReadAt(ctx context.Context, p []byte, inner uint64) (int, error) {
	return r.source.ReadAt(ctx, p, r.off+inner)
}

func TestBeforeCommitCompletionBoundary(t *testing.T) {
	for _, test := range []struct {
		name string
		size uint64
		kind sparse.RunKind
	}{
		{"empty", 0, sparse.Data}, {"data", 2*copyBufSize + 19, sparse.Data},
		{"hole", 4096, sparse.Hole}, {"zero", 4096, sparse.Zero},
	} {
		for _, encrypted := range []bool{false, true} {
			label := test.name + "/plain"
			if encrypted {
				label = test.name + "/encrypted"
			}
			t.Run(label, func(t *testing.T) {
				for _, fail := range []bool{false, true} {
					src := &commitSource{size: test.size, kind: test.kind}
					var output bytes.Buffer
					var opts []WriteOption
					var readOpts []ReadOption
					if encrypted {
						codec := WithCodec(&optionTestCodec{}, true)
						opts = append(opts, codec)
						readOpts = append(readOpts, codec)
					}
					calls := 0
					checkErr := errors.New("source changed")
					ctx := context.WithValue(context.Background(), struct{}{}, test.name)
					opts = append(opts, WithBeforeCommit(func(got context.Context) error {
						calls++
						if got != ctx {
							t.Fatal("check lost caller context")
						}
						wantRead := uint64(0)
						if test.kind == sparse.Data {
							wantRead = test.size
						}
						if src.read != wantRead {
							t.Fatalf("check before payload: %d/%d", src.read, wantRead)
						}
						if bytes.Contains(output.Bytes(), []byte(DigestMarkerPrefix)) {
							t.Fatal("marker preceded check")
						}
						if fail {
							return checkErr
						}
						return nil
					}))
					// Expose only Write: seekable output is not required.
					scheme, digest, err := WriteTo(ctx, struct{ io.Writer }{&output}, "image", src, opts...)
					if calls != 1 {
						t.Fatalf("check calls = %d", calls)
					}
					if fail {
						if !errors.Is(err, checkErr) || scheme != "" || digest != "" {
							t.Fatalf("failed result: %q %q %v", scheme, digest, err)
						}
						if bytes.Contains(output.Bytes(), []byte(DigestMarkerPrefix)) {
							t.Fatal("failed output has marker")
						}
						// SourceFrom is the full-consumption receiver. SourceAt
						// also supports legacy tar without a completion marker.
						received, _, receiveErr := SourceFrom(readerOnly{bytes.NewReader(output.Bytes())}, "image", readOpts...)
						if receiveErr == nil && test.size > 0 {
							_, receiveErr = received.ReadAt(ctx, make([]byte, test.size), 0)
						}
						if receiveErr == nil {
							t.Fatal("receiver accepted incomplete artifact")
						}
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					readOpts = append(readOpts, WithExpectedDigest(scheme, digest))
					accepted, _, err := SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "image", readOpts...)
					if err != nil {
						t.Fatal(err)
					}
					p := make([]byte, test.size)
					if len(p) > 0 {
						if _, err := accepted.ReadAt(ctx, p, 0); err != nil {
							t.Fatal(err)
						}
					}
					want := byte(0)
					if test.kind == sparse.Data {
						want = 'a'
					}
					if !bytes.Equal(p, bytes.Repeat([]byte{want}, len(p))) {
						t.Fatal("content changed")
					}
					if !encrypted {
						var original bytes.Buffer
						_, oldDigest, err := WriteTo(ctx, &original, "image", &commitSource{size: test.size, kind: test.kind})
						if err != nil || oldDigest != digest || !bytes.Equal(original.Bytes(), output.Bytes()) {
							t.Fatalf("check changed artifact identity: %v", err)
						}
					}
				}
			})
		}
	}
}

type commitErrorWriter struct{ err error }

func (w commitErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestBeforeCommitEarlyFailuresAndOptions(t *testing.T) {
	broken := errors.New("broken I/O")
	calls := 0
	check := WithBeforeCommit(func(context.Context) error { calls++; return nil })
	for _, test := range []struct {
		src sparse.Source
		w   io.Writer
	}{
		{&commitSource{size: 4096, kind: sparse.Data, err: broken}, io.Discard},
		{&commitSource{}, commitErrorWriter{broken}},
	} {
		if _, _, err := WriteTo(context.Background(), test.w, "image", test.src, check); !errors.Is(err, broken) {
			t.Fatalf("error = %v", err)
		}
	}
	if calls != 0 {
		t.Fatal("check ran after an earlier I/O failure")
	}
	if _, _, err := CarrierDigest("image", &commitSource{}, check); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("identity-only operation accepted a commit check: %v", err)
	}
	for _, options := range [][]WriteOption{{WithBeforeCommit(nil)}, {check, check}} {
		var output bytes.Buffer
		if _, _, err := WriteTo(context.Background(), &output, "image", &commitSource{}, options...); !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("option error = %v", err)
		}
		if output.Len() != 0 {
			t.Fatal("invalid options emitted bytes")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer
	_, _, err := WriteTo(ctx, &output, "image", &commitSource{}, WithBeforeCommit(func(ctx context.Context) error { return ctx.Err() }))
	if !errors.Is(err, context.Canceled) || bytes.Contains(output.Bytes(), []byte(DigestMarkerPrefix)) {
		t.Fatalf("cancelled check: %v", err)
	}
}
