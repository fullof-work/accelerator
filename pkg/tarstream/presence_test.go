package tarstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Retain the contiguous reference encoding for malformed carrier fixtures and
// byte comparisons with the segmented production writer.
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

func plannedPrefixReader(plan *writePlan) io.Reader {
	readers := make([]io.Reader, 0, len(plan.prefixParts))
	for _, part := range plan.prefixParts {
		readers = append(readers, bytes.NewReader(part))
	}
	return io.MultiReader(readers...)
}

func readPlannedPresence(plan *writePlan) ([]byte, uint64, error) {
	r := plannedPrefixReader(plan)
	if _, err := io.CopyN(io.Discard, r, 512); err != nil {
		return nil, 0, err
	}
	return readPresence(r, int64(presenceHeaderSize+len(plan.presence)))
}

type triCarrierSource struct {
	sparse.Source
	zeroStart, zeroEnd, payload uint64
}

func (s triCarrierSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.payload, [32]byte{}, false
}
func (s triCarrierSource) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }
func (s triCarrierSource) RunAt(off, limit uint64) (sparse.Run, error) {
	r, err := s.Source.RunAt(off, limit)
	if err != nil {
		return nil, err
	}
	kind, end := r.Kind(), r.End()
	if off < s.zeroStart {
		end = min(end, s.zeroStart)
	} else if off < s.zeroEnd {
		kind = sparse.Zero
		end = min(end, s.zeroEnd)
	}
	return triCarrierRun{s, off, end, kind}, nil
}

type triCarrierRun struct {
	s        triCarrierSource
	off, end uint64
	kind     sparse.RunKind
}

func (r triCarrierRun) Offset() uint64       { return r.off }
func (r triCarrierRun) End() uint64          { return r.end }
func (r triCarrierRun) Kind() sparse.RunKind { return r.kind }
func (r triCarrierRun) ReadAt(ctx context.Context, p []byte, inner uint64) (int, error) {
	return r.s.ReadAt(ctx, p, r.off+inner)
}

func presenceFixture(t *testing.T) (triCarrierSource, []byte) {
	t.Helper()
	body := make([]byte, 4*4096)
	copy(body[4096:8192], bytes.Repeat([]byte{0x11}, 4096))
	copy(body[12288:], bytes.Repeat([]byte{0x33}, 4096))
	body = append(body, []byte("metadata-tail")...)
	s, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), []sparse.Extent{{Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	return triCarrierSource{s, 8192, 12288, 16384}, body
}

func TestPresencePreservesLegacyIdentityAndTail(t *testing.T) {
	s, body := presenceFixture(t)
	var output bytes.Buffer
	_, digest, err := WriteTo(context.Background(), &output, "disk", s)
	if err != nil {
		t.Fatal(err)
	}
	const legacyDigest = "ab88a8a3d89236a3e81b16c28c7cb781b8dc7914c1fdb78e8f599c173f9e383f"
	if digest != legacyDigest {
		t.Fatal("physical Zero encoding changed logical identity", digest)
	}
	// Generated with upstream accelerator 9577691, before presence encoding.
	legacy, err := os.ReadFile("testdata/legacy-presence-zero.tar")
	if err != nil {
		t.Fatal(err)
	}
	if output.Len() != len(legacy)-4096+1024 {
		t.Fatal("unexpected omitted-payload size", output.Len(), len(legacy))
	}
	for _, old := range []bool{false, true} {
		artifact := output.Bytes()
		if old {
			artifact = legacy
		}
		for _, sequential := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%t/sequential=%t", old, sequential), func(t *testing.T) {
				var opened sparse.Source
				var name string
				var err error
				if sequential {
					opened, name, err = SourceFrom(readerOnly{bytes.NewReader(artifact)}, "", WithExpectedDigest(DigestScheme, digest))
				} else {
					opened, name, err = SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", WithExpectedDigest(DigestScheme, digest))
				}
				if err != nil || name != "disk" {
					t.Fatal(name, err)
				}
				if c, ok := opened.(io.Closer); ok {
					defer c.Close()
				}
				kinds := []sparse.RunKind{sparse.Hole, sparse.Data, sparse.Zero, sparse.Data, sparse.Data}
				if old {
					kinds[2] = sparse.Data
				}
				for i, want := range kinds {
					r, e := opened.RunAt(uint64(i*4096), 4096)
					if e != nil || r.Kind() != want {
						t.Fatal(i, r, e)
					}
				}
				got := make([]byte, len(body))
				if _, err := opened.ReadAt(context.Background(), got, 0); err != nil || !bytes.Equal(body, got) {
					t.Fatal("logical bytes", err)
				}
			})
		}
	}
}

func TestPresenceRejectsMalformedWithoutLegacyFallback(t *testing.T) {
	s, _ := presenceFixture(t)
	var output bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &output, "disk", s); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"checksum", "padding", "tail-bit", "payload-in-hole", "duplicate", "missing-bitmap", "bad-marker", "missing-identity"} {
		t.Run(mode, func(t *testing.T) {
			p := append([]byte(nil), output.Bytes()...)
			switch mode {
			case "checksum":
				p[512+64] ^= 1
			case "padding":
				p[512+65] = 1
			case "tail-bit", "payload-in-hole":
				b := byte(0x8e)
				if mode == "payload-in-hole" {
					b = 0x0c
				}
				copy(p[512:], presenceBody([]byte{b}, 16384))
			case "duplicate":
				p = append(append([]byte(nil), p[:1024]...), p...)
			case "missing-bitmap":
				p = p[1024:]
			case "bad-marker":
				i := bytes.Index(p, []byte("KUASAR.presence=1"))
				if i < 0 {
					t.Fatal("missing PAX")
				}
				p[i+len("KUASAR.presence=")] = '2'
			case "missing-identity":
				p = p[:len(p)-2048]
			}
			if _, _, err := SourceAt(bytes.NewReader(p), int64(len(p)), ""); err == nil {
				t.Fatal("random access accepted malformed presence")
			}
			opened, _, err := SourceFrom(readerOnly{bytes.NewReader(p)}, "")
			if err == nil {
				_, err = opened.ReadAt(context.Background(), make([]byte, opened.Size()), 0)
			}
			if err == nil {
				t.Fatal("sequential access accepted malformed presence")
			}
		})
	}
}

func TestPresenceAllZeroOrHoleCompletesVerification(t *testing.T) {
	for _, hole := range []bool{false, true} {
		body := make([]byte, 16384)
		var holes []sparse.Extent
		if hole {
			holes = []sparse.Extent{{Size: uint64(len(body))}}
		}
		base, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
		if err != nil {
			t.Fatal(err)
		}
		var s sparse.Source = base
		if !hole {
			s = triCarrierSource{base, 0, uint64(len(body)), uint64(len(body))}
		}
		var output bytes.Buffer
		if _, _, err := WriteTo(context.Background(), &output, "empty-pages", s); err != nil {
			t.Fatal(err)
		}
		if output.Len() > 8192 {
			t.Fatal("empty payload expanded", output.Len())
		}
		for _, bad := range []bool{false, true} {
			p := append([]byte(nil), output.Bytes()...)
			if bad {
				p = append(p, 1)
			}
			opened, _, err := SourceFrom(readerOnly{bytes.NewReader(p)}, "")
			if bad {
				if !errors.Is(err, ErrInvalidCanonicalTarstream) {
					t.Fatal("no-data source did not verify EOF", err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := opened.RunAt(0, opened.Size())
			want := sparse.Zero
			if hole {
				want = sparse.Hole
			}
			if err != nil || r.Kind() != want {
				t.Fatal(r, err)
			}
		}
	}
}

func TestLegacySubpageHolesStayByteExact(t *testing.T) {
	p, err := os.ReadFile("testdata/legacy-byte-holes.tar")
	if err != nil {
		t.Fatal(err)
	}
	v, err := ReadSeekFrom(bytes.NewReader(p), "")
	if err != nil {
		t.Fatal(err)
	}
	if holes := v.Holes(); len(holes) != 1 || holes[0] != (sparse.Extent{Offset: 13, Size: 4111}) {
		t.Fatal(holes)
	}
	body, err := io.ReadAll(v)
	if err != nil {
		t.Fatal(err)
	}
	const digest = "16543ec6631327ae59db96633d6a961f04ae6b90e01a5eb93399128b394bdfca"
	for _, sequential := range []bool{false, true} {
		var s sparse.Source
		if sequential {
			s, _, err = SourceFrom(readerOnly{bytes.NewReader(p)}, "", WithExpectedDigest(DigestScheme, digest))
		} else {
			s, _, err = SourceAt(bytes.NewReader(p), int64(len(p)), "", WithExpectedDigest(DigestScheme, digest))
		}
		if err != nil {
			t.Fatal(err)
		}
		hole, err := s.RunAt(13, s.Size()-13)
		if err != nil || hole.Kind() != sparse.Hole || hole.End() != 4124 {
			t.Fatal("legacy subpage hole changed", sequential, hole, err)
		}
		got := make([]byte, len(body))
		if n, err := s.ReadAt(context.Background(), got, 0); n != len(got) || err != nil || !bytes.Equal(got, body) {
			t.Fatal("legacy bytes changed", sequential, n, err)
		}
	}
	for _, encrypted := range []bool{false, true} {
		reader := &countingReaderAt{ReaderAt: bytes.NewReader(p)}
		s, _, err := SourceAt(reader, int64(len(p)), "")
		if err != nil {
			t.Fatal(err)
		}
		beforeRead := reader.read
		var output bytes.Buffer
		calls := 0
		options := []WriteOption{WithBeforeCommit(func(context.Context) error { calls++; return nil })}
		if encrypted {
			options = append(options, WithCodec(&optionTestCodec{}, true))
		}
		_, _, err = WriteTo(context.Background(), &output, "bytes", s, options...)
		if err == nil || !strings.Contains(err.Error(), "presence hole") || output.Len() != 0 || reader.read != beforeRead || calls != 0 {
			t.Fatal("unrepresentable legacy source was not rejected before I/O", encrypted, output.Len(), reader.read-beforeRead, calls, err)
		}
	}
}
