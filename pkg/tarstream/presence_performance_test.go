package tarstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestPresenceRunEndMatchesBitScan(t *testing.T) {
	const pages = uint64(137)
	for pattern := 0; pattern < 256; pattern++ {
		bitmap := make([]byte, (pages+7)/8)
		for i := range bitmap {
			bitmap[i] = byte(pattern ^ (i * 17))
		}
		for page := uint64(0); page < pages; page++ {
			for bound := page + 1; bound <= pages; bound++ {
				want := page + 1
				for want < bound && presenceBit(bitmap, want) == presenceBit(bitmap, page) {
					want++
				}
				if got := presenceRunEnd(bitmap, page, bound); got != want {
					t.Fatalf("pattern=%02x page=%d bound=%d end=%d, want %d", pattern, page, bound, got, want)
				}
			}
		}
	}
}

func TestPresenceSetRangePreservesOtherBits(t *testing.T) {
	for start := uint64(0); start <= 137; start++ {
		for end := start; end <= 137; end++ {
			got := bytes.Repeat([]byte{0x33}, 18)
			want := append([]byte(nil), got...)
			for page := start; page < end; page++ {
				want[page/8] |= 1 << (page % 8)
			}
			setPresenceRange(got, start, end)
			if !bytes.Equal(got, want) {
				t.Fatalf("range [%d,%d): bitmap differs", start, end)
			}
		}
	}
}

func TestPresenceBatchedLayoutIdentity(t *testing.T) {
	// The largest case has more than one encoding batch and a partial page.
	for _, payload := range []uint64{0, 1, 4095, 4096, 4097, 32767, 32768, 32769, 4<<20 + 37} {
		for _, pattern := range []byte{0, 0xff, 0x55, 0x81, 0x3c} {
			bitmap := bytes.Repeat([]byte{pattern}, int((presencePages(payload)+7)/8))
			if tail := presencePages(payload) % 8; tail != 0 {
				bitmap[len(bitmap)-1] &= byte((1 << tail) - 1)
			}
			var extents []extent
			var total int64
			for page := uint64(0); page < presencePages(payload); {
				start := page
				present := presenceBit(bitmap, page)
				for page < presencePages(payload) && presenceBit(bitmap, page) == present {
					page++
				}
				if present {
					end := min(page*presencePage, payload)
					e := extent{Offset: int64(start * presencePage), Size: int64(end - start*presencePage)}
					extents = append(extents, e)
					total += e.Size
				}
			}
			want := newPayloadHasher(payload, extents)
			got, gotTotal := presencePayloadHasher(payload, bitmap)
			if gotTotal != total || !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
				t.Fatalf("payload=%d pattern=%02x: logical identity changed", payload, pattern)
			}
		}
	}
}

func TestPresencePrefixMatchesCanonicalEncoding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload uint64
		tail    uint64
		kind    sparse.RunKind
	}{
		{"empty", 0, 0, sparse.Hole},
		{"partial-zero", 5001, 17, sparse.Zero},
		{"partial-hole", 32769, 0, sparse.Hole},
		{"one-tib", maxPresencePayload, 17, sparse.Hole},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &presencePlanSource{size: tc.payload + tc.tail, payload: tc.payload, kind: tc.kind}
			plan, err := makeWritePlan(strings.Repeat("long-name/", 12)+"disk", source)
			if err != nil {
				t.Fatal(err)
			}
			// Reference the previous contiguous body encoding without copying
			// the complete bitmap into another full carrier prefix.
			body := presenceBody(plan.presence, tc.payload)
			header := ustarBlock(rawHeader{name: presenceName, mode: 0o444, size: int64(len(body)), typeflag: '0'})
			pad := (512 - len(body)%512) % 512
			payload := payloadPrefix(plan.name, plan.logical, plan.stored, plan.sparseMap, plan.payloadSize, true)
			want := sha256.New()
			for _, part := range [][]byte{header[:], body, zeroBlock[:pad], payload} {
				want.Write(part)
			}
			got := sha256.New()
			if err := plan.writePrefix(got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
				t.Fatal("segmented prefix changed canonical bytes")
			}
			if wantSize := int64(512 + len(body) + pad + len(payload)); plan.packedStart != wantSize {
				t.Fatalf("packed start=%d, want %d", plan.packedStart, wantSize)
			}
			if source.reads != 0 {
				t.Fatal("prefix planning read payload")
			}
		})
	}
}

type prefixErrorWriter struct {
	remaining int64
	written   int64
	err       error
}

func (w *prefixErrorWriter) Write(p []byte) (int, error) {
	n := min(int64(len(p)), w.remaining)
	w.remaining -= n
	w.written += n
	if n < int64(len(p)) {
		return int(n), w.err
	}
	return int(n), nil
}

func TestPresencePrefixPropagatesWriteFailures(t *testing.T) {
	plan, err := makeWritePlan("disk", &presencePlanSource{size: 5, payload: 5, kind: sparse.Zero})
	if err != nil {
		t.Fatal(err)
	}
	broken := errors.New("prefix writer failed")
	for _, limit := range []int64{0, 511, 512, 575, 576, 577, 1023, 1024, plan.packedStart - 1} {
		for _, failure := range []error{broken, nil} {
			w := &prefixErrorWriter{remaining: limit, err: failure}
			want := failure
			if want == nil {
				want = io.ErrShortWrite
			}
			if err := plan.writePrefix(w); !errors.Is(err, want) || w.written != limit {
				t.Fatalf("limit=%d: written=%d error=%v, want %v", limit, w.written, err, want)
			}
		}
	}
}

func TestPresenceSegmentedPrefixKeepsEncryptedGeometry(t *testing.T) {
	fixture, _ := presenceFixture(t)
	for _, tc := range []struct {
		name          string
		source        sparse.Source
		alignedPrefix bool
		crossesRecord bool
	}{
		{name: "tri-state", source: fixture},
		{
			name:          "32MiB-hole",
			source:        &presencePlanSource{size: 32 << 20, payload: 32 << 20, kind: sparse.Hole},
			alignedPrefix: true,
		},
		{
			name:          "128MiB-hole",
			source:        &presencePlanSource{size: 128 << 20, payload: 128 << 20, kind: sparse.Hole},
			crossesRecord: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := makeWritePlan("disk", tc.source)
			if err != nil {
				t.Fatal(err)
			}
			var plaintext bytes.Buffer
			if _, _, err := WriteTo(context.Background(), &plaintext, "disk", tc.source); err != nil {
				t.Fatal(err)
			}
			geometry, err := makeEnvelopeHeader(plan)
			if err != nil {
				t.Fatal(err)
			}
			if tc.alignedPrefix && (plan.packedStart%recordSize != 0 || geometry.firstSize != recordSize) {
				t.Fatalf("prefix does not end at a full record: packedStart=%d firstSize=%d", plan.packedStart, geometry.firstSize)
			}
			if tc.crossesRecord {
				const presenceStart = 512 + presenceHeaderSize
				presenceEnd := presenceStart + len(plan.presence)
				if presenceStart >= int(geometry.firstSize) || presenceEnd <= int(geometry.firstSize) {
					t.Fatalf("bitmap [%d,%d) does not cross first record ending at %d", presenceStart, presenceEnd, geometry.firstSize)
				}
			}
			prefix := clearPrefix([32]byte{})
			header := geometry.marshal()
			var contiguous, segmented bytes.Buffer
			first := newRecordWriter(&contiguous, &optionTestCodec{}, prefix, header, geometry)
			if err := writeFull(first, plaintext.Bytes()); err != nil {
				t.Fatal(err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			second := newRecordWriter(&segmented, &optionTestCodec{}, prefix, header, geometry)
			if _, err := emitPlan(context.Background(), second, plan, tc.source, nil); err != nil {
				t.Fatal(err)
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(contiguous.Bytes(), segmented.Bytes()) {
				t.Fatal("segmentation changed encrypted record framing")
			}
		})
	}
}

func TestPresencePlanningClipsRunsAtPayloadBoundary(t *testing.T) {
	for _, payload := range []uint64{0, 5, 5001} {
		for _, zero := range []bool{false, true} {
			t.Run(fmt.Sprintf("payload=%d/zero=%t", payload, zero), func(t *testing.T) {
				const tail = uint64(17)
				size := payload + tail
				source := triCarrierSource{
					Source:    sparse.Dense(bytes.NewReader(make([]byte, size)), size),
					zeroStart: size, zeroEnd: size, payload: payload,
				}
				if zero {
					source.zeroStart = 0
				}
				plan, err := makeWritePlan("disk", source)
				if err != nil {
					t.Fatal(err)
				}
				if plan.presence == nil || len(plan.presence) != int((presencePages(payload)+7)/8) {
					t.Fatal("payload boundary changed bitmap length")
				}
				for page := uint64(0); page < presencePages(payload); page++ {
					if !presenceBit(plan.presence, page) {
						t.Fatal("payload page lost presence")
					}
				}
				if bits := presencePages(payload) % 8; bits != 0 && plan.presence[len(plan.presence)-1]>>bits != 0 {
					t.Fatal("tail set bits beyond payload")
				}
				wantPacked := int64(size)
				if zero {
					wantPacked -= int64(payload / presencePage * presencePage)
				}
				if plan.packedSize != wantPacked {
					t.Fatalf("packed=%d, want %d", plan.packedSize, wantPacked)
				}
			})
		}
	}
}

type changingPresenceSource struct{ presencePlanSource }

func (s *changingPresenceSource) RunAt(off, limit uint64) (sparse.Run, error) {
	s.runs++
	end := min(off+limit, s.size)
	kind := sparse.Data
	if s.runs > 1 {
		if off == 0 {
			end = 1
		} else {
			kind = sparse.Hole
		}
	}
	return tarSourceRun[*changingPresenceSource]{source: s, offset: off, end: end, kind: kind}, nil
}

func TestPresenceChangedHoleFailsBeforeOutput(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		source := &changingPresenceSource{presencePlanSource: presencePlanSource{size: 8192, payload: 8192}}
		var output bytes.Buffer
		var options []WriteOption
		if encrypted {
			options = append(options, WithCodec(&optionTestCodec{}, true))
		}
		_, _, err := WriteTo(context.Background(), &output, "disk", source, options...)
		if err == nil || !strings.Contains(err.Error(), "presence changed") || output.Len() != 0 || source.reads != 0 {
			t.Fatalf("encrypted=%t: error=%v output=%d reads=%d", encrypted, err, output.Len(), source.reads)
		}
	}
}
