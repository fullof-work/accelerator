package tarstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

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
	s, _, err := SourceAt(bytes.NewReader(p), int64(len(p)), "")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	_, digest, err := WriteTo(context.Background(), &output, "bytes", s)
	if err != nil || digest != "16543ec6631327ae59db96633d6a961f04ae6b90e01a5eb93399128b394bdfca" || !bytes.Equal(p, output.Bytes()) {
		t.Fatal("legacy subpage fallback changed bytes/identity", digest, err)
	}
}
