package crypto

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

type zeroCarrier struct{ size uint64 }

func (z zeroCarrier) Size() uint64 { return z.size }
func (z zeroCarrier) ReadAt(context.Context, []byte, uint64) (int, error) {
	return 0, errors.New("writer read omitted Zero payload")
}
func (z zeroCarrier) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= z.size {
		return nil, io.EOF
	}
	return zeroCarrierRun{off, min(z.size, off+limit)}, nil
}

type zeroCarrierRun struct{ off, end uint64 }

func (r zeroCarrierRun) Offset() uint64       { return r.off }
func (r zeroCarrierRun) End() uint64          { return r.end }
func (r zeroCarrierRun) Kind() sparse.RunKind { return sparse.Zero }
func (r zeroCarrierRun) ReadAt(context.Context, []byte, uint64) (int, error) {
	return 0, errors.New("writer read omitted Zero run")
}

func TestEncryptedZeroPresenceAuthenticatesBeforeClassification(t *testing.T) {
	codec, err := NewTarStreamCodec([32]byte{42})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	scheme, digest, err := tarstream.WriteTo(context.Background(), &out, "zero", zeroCarrier{4 << 20}, tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() > 8192 {
		t.Fatal("Zero expanded", out.Len())
	}
	for _, sequential := range []bool{false, true} {
		open := func(data []byte) (sparse.Source, error) {
			opts := []tarstream.ReadOption{tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest)}
			if sequential {
				s, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(data)}, "", opts...)
				return s, err
			}
			s, _, err := tarstream.SourceAt(bytes.NewReader(data), int64(len(data)), "", opts...)
			return s, err
		}
		s, err := open(out.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.RunAt(0, s.Size())
		if err != nil || r.Kind() != sparse.Zero || r.End() != s.Size() {
			t.Fatal(r, err)
		}
		for _, offset := range []int{129 + 1 + 512 + 64, out.Len() - 1} {
			bad := append([]byte(nil), out.Bytes()...)
			bad[offset] ^= 1
			if _, err := open(bad); !errors.Is(err, tarstream.ErrAuthentication) {
				t.Fatal("unauthenticated presence or end accepted", sequential, offset, err)
			}
		}
		if _, err := open(append(append([]byte(nil), out.Bytes()...), 1)); err == nil {
			t.Fatal("no-data carrier accepted outer trailing bytes", sequential)
		}
	}
}
