package tarstream

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func BenchmarkPresenceWritePlan(b *testing.B) {
	for _, payload := range []uint64{32 << 30, 1 << 40} {
		b.Run(fmt.Sprintf("payload=%dGiB", payload>>30), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				source := &presencePlanSource{size: payload, payload: payload, kind: sparse.Hole}
				plan, err := makeWritePlanLimited(context.Background(), "disk", source, 0)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(source.runs), "RunAt/op")
				runtime.KeepAlive(plan)
			}
		})
	}
}

func BenchmarkPresencePayloadHasher(b *testing.B) {
	for _, payload := range []uint64{32 << 30, 1 << 40} {
		for _, pattern := range []byte{0, 0xff, 0x55} {
			bitmap := bytes.Repeat([]byte{pattern}, int(payload/32768))
			b.Run(fmt.Sprintf("payload=%dGiB/pattern=%02x", payload>>30, pattern), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					h, _ := presencePayloadHasher(payload, bitmap)
					runtime.KeepAlive(h)
				}
			})
		}
	}
}

func BenchmarkPresenceRunEnd(b *testing.B) {
	bitmap := bytes.Repeat([]byte{0xff}, 32<<20)
	const pages = uint64(1 << 28)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if end := presenceRunEnd(bitmap, 0, pages); end != pages {
			b.Fatal(end)
		}
	}
}

func BenchmarkPresenceSetRange(b *testing.B) {
	bitmap := make([]byte, 32<<20)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		setPresenceRange(bitmap, 0, 1<<28)
	}
	runtime.KeepAlive(bitmap)
}

func BenchmarkPresenceFragmentedWritePlan(b *testing.B) {
	const payload = uint64(1 << 30)
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		source := &presencePlanSource{size: payload, payload: payload, alternating: true}
		plan, err := makeWritePlanLimited(context.Background(), "disk", source, 0)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(source.runs), "RunAt/op")
		runtime.KeepAlive(plan)
	}
}
