package tarstream_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// The logical image is a hole followed by one 4 KiB data page. No logical-size
// buffer is allocated. This benchmark uses public APIs so the same harness can
// measure both the baseline and the optimized reader.
type benchmarkGapReaderAt struct{ size int64 }

func (r benchmarkGapReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative read offset %d", offset)
	}
	if offset >= r.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.size-offset))
	clear(p[:n])
	start := max(int64(0), r.size-4096-offset)
	if start < int64(n) {
		for i := int(start); i < n; i++ {
			p[i] = 0xa5
		}
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func BenchmarkSequentialSourceGap(b *testing.B) {
	for _, payload := range []uint64{32 << 30, 128 << 30, 1 << 40} {
		b.Run(fmt.Sprintf("logical=%dGiB", payload>>30), func(b *testing.B) {
			b.StopTimer()
			source, err := sparse.NewSource(benchmarkGapReaderAt{int64(payload)}, payload, []sparse.Extent{{Size: payload - 4096}})
			if err != nil {
				b.Fatal(err)
			}
			var wire bytes.Buffer
			_, digest, err := tarstream.WriteTo(context.Background(), &wire, "disk", source)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(4096)
			b.ResetTimer()
			var got [4096]byte
			for n := 0; n < b.N; n++ {
				// Opening and its bitmap allocation are excluded; the timed
				// read includes skipped stored data, identity and trailer checks.
				opened, _, err := tarstream.SourceFrom(bytes.NewReader(wire.Bytes()), "disk", tarstream.WithExpectedDigest(tarstream.DigestScheme, digest))
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				read, err := opened.ReadAt(context.Background(), got[:], payload-4096)
				b.StopTimer()
				if err != nil || read != len(got) {
					b.Fatalf("final data read: n=%d err=%v", read, err)
				}
				for _, value := range got {
					if value != 0xa5 {
						b.Fatal("wrong final data page")
					}
				}
			}
		})
	}
}
