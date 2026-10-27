package tarstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const advanceFixtureDigest = "ab88a8a3d89236a3e81b16c28c7cb781b8dc7914c1fdb78e8f599c173f9e383f"

type advanceArtifact struct {
	name string
	wire []byte
}

func advanceArtifacts(t *testing.T) []advanceArtifact {
	t.Helper()
	source, _ := presenceFixture(t)
	var wire bytes.Buffer
	if _, digest, err := WriteTo(context.Background(), &wire, "disk", source); err != nil || digest != advanceFixtureDigest {
		t.Fatalf("presence fixture identity: digest=%s err=%v", digest, err)
	}
	legacy, err := os.ReadFile("testdata/legacy-presence-zero.tar")
	if err != nil {
		t.Fatal(err)
	}
	return []advanceArtifact{{"presence", wire.Bytes()}, {"legacy", legacy}}
}

func advancePackedBounds(t *testing.T, wire []byte) (int64, int64) {
	t.Helper()
	r := bytes.NewReader(wire)
	m, err := locate(r, "disk", seekSkip(r))
	if err != nil {
		t.Fatal(err)
	}
	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	return start, m.stored - m.mapLen
}

func TestSequentialAdvanceValidatesSkippedDataAndTail(t *testing.T) {
	const tail = "metadata-tail"
	for _, artifact := range advanceArtifacts(t) {
		packedStart, packedSize := advancePackedBounds(t, artifact.wire)
		for _, mode := range []string{"valid", "skipped-data", "tail", "trailer", "truncated-trailer", "extra-byte", "expected-digest"} {
			t.Run(artifact.name+"/"+mode, func(t *testing.T) {
				wire := bytes.Clone(artifact.wire)
				expected := advanceFixtureDigest
				var want error
				switch mode {
				case "skipped-data":
					wire[packedStart] ^= 1
					want = ErrDigestMismatch
				case "tail":
					wire[packedStart+packedSize-int64(len(tail))] ^= 1
					want = ErrDigestMismatch
				case "trailer":
					wire[len(wire)-1] = 1
					want = ErrInvalidCanonicalTarstream
				case "truncated-trailer":
					wire = wire[:len(wire)-1]
					want = ErrInvalidCanonicalTarstream
				case "extra-byte":
					wire = append(wire, 0)
					want = ErrInvalidCanonicalTarstream
				case "expected-digest":
					expected = "0000000000000000000000000000000000000000000000000000000000000000"
					want = ErrDigestMismatch
				}
				source, _, err := SourceFrom(readerOnly{bytes.NewReader(wire)}, "disk", WithExpectedDigest(DigestScheme, expected))
				if err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(tail))
				n, err := source.ReadAt(context.Background(), got, 16384)
				if !errors.Is(err, want) {
					t.Fatalf("final read: n=%d err=%v, want %v", n, err, want)
				}
				if want == nil && (n != len(tail) || string(got) != tail) {
					t.Fatalf("tail: n=%d data=%q", n, got)
				}
				view := source.(*sourceView).seq
				if view.packedRead != packedSize || !view.finished {
					t.Fatalf("verification bypassed: packed=%d/%d finished=%t", view.packedRead, packedSize, view.finished)
				}
			})
		}
	}
}

func TestSequentialAdvanceFinishesBeforeTrailingOmittedBytes(t *testing.T) {
	for _, kind := range []sparse.RunKind{sparse.Hole, sparse.Zero} {
		body := make([]byte, 12288)
		copy(body[4096:8192], bytes.Repeat([]byte{0x33}, 4096))
		holes := []sparse.Extent{{Size: 4096}}
		if kind == sparse.Hole {
			holes = append(holes, sparse.Extent{Offset: 8192, Size: 4096})
		}
		base, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
		if err != nil {
			t.Fatal(err)
		}
		var original sparse.Source = base
		if kind == sparse.Zero {
			original = triCarrierSource{base, 8192, 12288, 12288}
		}
		var wire bytes.Buffer
		_, digest, err := WriteTo(context.Background(), &wire, "disk", original)
		if err != nil {
			t.Fatal(err)
		}
		for _, corrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%v/corrupt=%t", kind, corrupt), func(t *testing.T) {
				data := bytes.Clone(wire.Bytes())
				if corrupt {
					data[len(data)-1] = 1
				}
				source, _, err := SourceFrom(readerOnly{bytes.NewReader(data)}, "disk", WithExpectedDigest(DigestScheme, digest))
				if err != nil {
					t.Fatal(err)
				}
				got := []byte{0xff}
				n, err := source.ReadAt(context.Background(), got, uint64(len(body)-1))
				if corrupt {
					// The last stored Read fulfills CopyN's count while also
					// returning the trailer failure. It must not be swallowed.
					if n != 0 || !errors.Is(err, ErrInvalidCanonicalTarstream) || got[0] != 0xff {
						t.Fatalf("exposed bytes after verification failure: n=%d err=%v data=%x", n, err, got)
					}
				} else if n != 1 || err != nil || got[0] != 0 {
					t.Fatalf("trailing omitted byte: n=%d err=%v data=%x", n, err, got)
				}
				view := source.(*sourceView).seq
				if !view.finished || view.packedRead != view.stored-view.mapLen {
					t.Fatal("trailing logical skip did not finish stored-data verification")
				}
			})
		}
	}
}

// Metadata reads complete normally. At the first stored byte, return one
// permitted empty read, followed by small successful reads of the carrier.
type advanceChunkReader struct {
	*bytes.Reader
	at       int64
	injected bool
}

func (r *advanceChunkReader) Read(p []byte) (int, error) {
	pos := r.Size() - int64(r.Len())
	if pos == r.at && !r.injected {
		r.injected = true
		return 0, nil
	}
	if pos >= r.at && len(p) > 7 {
		p = p[:7]
	}
	return r.Reader.Read(p)
}

func TestSequentialAdvancePreservesShortReadSemantics(t *testing.T) {
	for _, artifact := range advanceArtifacts(t) {
		packedStart, _ := advancePackedBounds(t, artifact.wire)
		t.Run(artifact.name+"/empty-then-short-reads", func(t *testing.T) {
			r := &advanceChunkReader{Reader: bytes.NewReader(artifact.wire), at: packedStart}
			source, _, err := SourceFrom(r, "disk", WithExpectedDigest(DigestScheme, advanceFixtureDigest))
			if err != nil {
				t.Fatal(err)
			}
			var got [13]byte
			n, err := source.ReadAt(context.Background(), got[:], 16384)
			if err != nil || n != len(got) || string(got[:]) != "metadata-tail" || !r.injected {
				t.Fatalf("read after (0,nil): n=%d err=%v data=%q injected=%t", n, err, got, r.injected)
			}
		})
		t.Run(artifact.name+"/truncated-skipped-data", func(t *testing.T) {
			wire := artifact.wire[:packedStart+4095]
			source, _, err := SourceFrom(readerOnly{bytes.NewReader(wire)}, "disk")
			if err != nil {
				t.Fatal(err)
			}
			var got [13]byte
			if n, err := source.ReadAt(context.Background(), got[:], 16384); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncated skipped data: n=%d err=%v", n, err)
			}
		})
		for _, cause := range []error{io.EOF, readerr.Mark(io.EOF, true), readerr.Mark(io.EOF, false), context.Canceled} {
			t.Run(artifact.name+"/data-and-error/"+cause.Error(), func(t *testing.T) {
				r := &failedFieldReader{Reader: bytes.NewReader(artifact.wire), at: packedStart, cause: cause}
				source, _, err := SourceFrom(r, "disk", WithExpectedDigest(DigestScheme, advanceFixtureDigest))
				if err != nil {
					t.Fatal(err)
				}
				var got [13]byte
				n, err := source.ReadAt(context.Background(), got[:], 16384)
				if cause == io.EOF {
					// The plaintext MultiReader ends this input on a bare
					// EOF, even with data. Remaining carrier bytes cannot be
					// recovered; the next stored read must report truncation.
					if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("premature bare EOF: n=%d err=%v data=%q", n, err, got)
					}
				} else if n != 0 || !errors.Is(err, cause) || readerr.IsPermanent(err) != readerr.IsPermanent(cause) {
					t.Fatalf("stored read cause lost: n=%d err=%v, want %v", n, err, cause)
				}
			})
		}
	}
}

func TestSequentialAdvanceCanceledReadDoesNotConsume(t *testing.T) {
	for _, artifact := range advanceArtifacts(t) {
		t.Run(artifact.name, func(t *testing.T) {
			r := bytes.NewReader(artifact.wire)
			source, _, err := SourceFrom(readerOnly{r}, "disk", WithExpectedDigest(DigestScheme, advanceFixtureDigest))
			if err != nil {
				t.Fatal(err)
			}
			remaining := r.Len()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var got [13]byte
			n, err := source.ReadAt(ctx, got[:], 16384)
			view := source.(*sourceView).seq
			if n != 0 || !errors.Is(err, context.Canceled) || view.pos != 0 || view.packedRead != 0 || r.Len() != remaining {
				t.Fatalf("canceled read consumed input: n=%d err=%v pos=%d packed=%d", n, err, view.pos, view.packedRead)
			}
			if n, err := source.ReadAt(context.Background(), got[:], 16384); err != nil || n != len(got) || string(got[:]) != "metadata-tail" {
				t.Fatalf("read after cancellation: n=%d err=%v data=%q", n, err, got)
			}
		})
	}
}
