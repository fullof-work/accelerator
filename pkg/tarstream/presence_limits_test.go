package tarstream

import (
	stdtar "archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Large logical sources expose metadata only. Tests must fail if planning reads
// their payload, and must not allocate or hash the declared logical capacity.
type presencePlanSource struct {
	size, payload uint64
	kind          sparse.RunKind
	holes         []sparse.Extent
	alternating   bool
	reads, runs   int
}

func (s *presencePlanSource) Size() uint64 { return s.size }
func (s *presencePlanSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.payload, [32]byte{}, false
}
func (*presencePlanSource) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }
func (s *presencePlanSource) ReadAt(context.Context, []byte, uint64) (int, error) {
	s.reads++
	return 0, errors.New("unexpected payload read during presence preflight")
}
func (s *presencePlanSource) RunAt(off, limit uint64) (sparse.Run, error) {
	s.runs++
	if off >= s.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, errors.New("zero run limit")
	}
	end, kind := off+min(limit, s.size-off), sparse.Data
	if off < s.payload {
		end, kind = min(end, s.payload), s.kind
		if s.alternating {
			end = min(end, (off/4096+1)*4096)
			kind = sparse.Zero
			if off/4096%2 != 0 {
				kind = sparse.Hole
			}
		}
		for _, hole := range s.holes {
			if off < hole.Offset {
				end = min(end, hole.Offset)
				break
			}
			if off < hole.Offset+hole.Size {
				end, kind = min(end, hole.Offset+hole.Size), sparse.Hole
				break
			}
		}
	}
	return tarSourceRun[*presencePlanSource]{source: s, offset: off, end: end, kind: kind}, nil
}

func TestPresenceWriterRejectsUnrepresentableBeforeOutput(t *testing.T) {
	for _, tc := range []struct {
		name          string
		size, payload uint64
		holes         []sparse.Extent
		wantError     string
	}{
		{name: "over-limit", size: 1<<40 + 4096, payload: 1<<40 + 4096, wantError: "exceeds presence limit"},
		{name: "subpage-hole-start", size: 8192, payload: 8192, holes: []sparse.Extent{{Offset: 1, Size: 4095}}, wantError: "presence hole"},
		{name: "subpage-hole-end", size: 8192, payload: 8192, holes: []sparse.Extent{{Size: 4095}}, wantError: "presence hole"},
	} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encrypted=%t", tc.name, encrypted), func(t *testing.T) {
				source := &presencePlanSource{size: tc.size, payload: tc.payload, kind: sparse.Data, holes: tc.holes}
				var output bytes.Buffer
				calls := 0
				options := []WriteOption{WithBeforeCommit(func(context.Context) error { calls++; return nil })}
				if encrypted {
					options = append(options, WithCodec(&optionTestCodec{}, true))
				}
				scheme, digest, err := WriteTo(context.Background(), &output, "disk", source, options...)
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error = %v, want %q", err, tc.wantError)
				}
				if output.Len() != 0 || source.reads != 0 || calls != 0 || scheme != "" || digest != "" {
					t.Fatalf("invalid input performed work: output=%d reads=%d callback=%d result=%s:%s", output.Len(), source.reads, calls, scheme, digest)
				}
				if tc.name == "over-limit" && source.runs != 0 {
					t.Fatal("oversized payload was traversed before rejecting its size")
				}
			})
		}
	}
}

func TestPresenceAboveFormerLimitKeepsFragmentedZeros(t *testing.T) {
	const size = uint64(32<<30 + 4096)
	source := &presencePlanSource{size: size, payload: size, alternating: true}
	plan, err := makeWritePlan("disk", source)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.presence) != 1<<20+1 || len(plan.extents) != 0 || plan.packedSize != 0 || source.reads != 0 {
		t.Fatalf("fragmented Zero/Hole expanded: bitmap=%d extents=%d packed=%d reads=%d", len(plan.presence), len(plan.extents), plan.packedSize, source.reads)
	}
	if plan.presence[0] != 0x55 || plan.presence[len(plan.presence)-1] != 1 {
		t.Fatal("fragmented page states changed")
	}
}

func TestPresenceOneTiBLimitExcludesDenseMetadataTail(t *testing.T) {
	const payload = uint64(1 << 40)
	const tail = uint64(17)
	source := &presencePlanSource{size: payload + tail, payload: payload, kind: sparse.Hole}
	plan, err := makeWritePlan("disk", source)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.presence) != 32<<20 || plan.payloadSize != int64(payload) || plan.logical != int64(payload+tail) || plan.packedSize != int64(tail) || source.reads != 0 {
		t.Fatalf("1 TiB plan: bitmap=%d payload=%d logical=%d packed=%d reads=%d", len(plan.presence), plan.payloadSize, plan.logical, plan.packedSize, source.reads)
	}
	if len(plan.extents) != 1 || plan.extents[0] != (extent{Offset: int64(payload), Size: int64(tail)}) {
		t.Fatalf("metadata tail extent = %v", plan.extents)
	}
	// Parse the actual planned presence member. This verifies the full 32 MiB
	// bitmap boundary without materializing the 1 TiB logical hole.
	bitmap, declared, err := readPlannedPresence(plan)
	if err != nil || declared != payload || !bytes.Equal(bitmap, plan.presence) {
		t.Fatalf("1 TiB reader boundary: payload=%d bitmap=%d error=%v", declared, len(bitmap), err)
	}
	if _, err := makeEnvelopeHeader(plan); err != nil {
		t.Fatalf("encrypted envelope rejected the 1 TiB plan: %v", err)
	}
	if _, _, err := readPresence(strings.NewReader(""), presenceHeaderSize+(32<<20)+1); !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("oversized bitmap must fail before reading its body: %v", err)
	}
}

func TestPresenceWriterAlwaysEmitsNewFormat(t *testing.T) {
	for _, tc := range []struct {
		name string
		size uint64
		kind sparse.RunKind
	}{
		{"empty", 0, sparse.Data},
		{"partial-data", 5, sparse.Data},
		{"data", 4096, sparse.Data},
		{"zero", 4096, sparse.Zero},
		{"hole", 4096, sparse.Hole},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			source := &commitSource{size: tc.size, kind: tc.kind}
			if _, _, err := WriteTo(context.Background(), &output, "disk", source); err != nil {
				t.Fatal(err)
			}
			reader := stdtar.NewReader(bytes.NewReader(output.Bytes()))
			header, err := reader.Next()
			if err != nil || header.Name != presenceName {
				t.Fatalf("first member is not presence: %v, %v", header, err)
			}
			header, err = reader.Next()
			if err != nil || header.Name != "disk" || header.PAXRecords[presencePAX] != "1" {
				t.Fatalf("payload lacks presence declaration: %v, %v", header, err)
			}
		})
	}
}

func TestPresencePartialPageBitmapBoundaries(t *testing.T) {
	for _, tc := range []struct {
		payload uint64
		bytes   int
	}{
		{0, 0}, {1, 1}, {4095, 1}, {4096, 1}, {4097, 1},
		{32767, 1}, {32768, 1}, {32769, 2},
	} {
		t.Run(fmt.Sprint(tc.payload), func(t *testing.T) {
			source := &presencePlanSource{size: tc.payload, payload: tc.payload, kind: sparse.Hole}
			plan, err := makeWritePlan("disk", source)
			if err != nil {
				t.Fatal(err)
			}
			if plan.presence == nil || len(plan.presence) != tc.bytes || plan.logical != int64(tc.payload) || source.reads != 0 {
				t.Fatalf("partial-page plan: bitmap=%v logical=%d reads=%d", plan.presence, plan.logical, source.reads)
			}
			bitmap, payload, err := readPlannedPresence(plan)
			if err != nil || bitmap == nil || len(bitmap) != tc.bytes || payload != tc.payload {
				t.Fatalf("partial-page presence: payload=%d bitmap=%v error=%v", payload, bitmap, err)
			}
		})
	}
	for _, tc := range []struct {
		payload uint64
		bitmap  []byte
	}{
		{0, []byte{0}},    // An empty payload has no bitmap bytes.
		{5001, []byte{4}}, // Only the two logical pages may have bits set.
	} {
		body := presenceBody(tc.bitmap, tc.payload)
		stored := len(body)
		body = append(body, make([]byte, (512-stored%512)%512)...)
		if _, _, err := readPresence(bytes.NewReader(body), int64(stored)); !errors.Is(err, ErrInvalidCanonicalTarstream) {
			t.Fatalf("unused bitmap bytes/bits accepted for payload %d: %v", tc.payload, err)
		}
	}
}

func TestPresenceArbitraryPayloadRoundTrip(t *testing.T) {
	type expectedRun struct {
		start, end uint64
		kind       sparse.RunKind
	}
	// Fixed digest values follow the pre-presence logical identity contract.
	// A partial final page must not add bytes or change a Hole into Zero.
	for _, tc := range []struct {
		name               string
		body               []byte
		payload            uint64
		holes              []sparse.Extent
		zeroStart, zeroEnd uint64
		runs               []expectedRun
		digest             string
	}{
		{name: "empty", digest: "0f1b8fbbc6dd67cabd6fb6eab06b934a8cd9d73b3edb19fb5aca5b9db227a06e"},
		{name: "empty-payload-with-tail", body: []byte("metadata-tail"), runs: []expectedRun{{0, 13, sparse.Data}}, digest: "53eb85a3254a2f29c282128cc091dae029b9edc883bec763e462ebe3d94b6065"},
		{name: "data", body: bytes.Repeat([]byte{'d'}, 5001), payload: 5001, runs: []expectedRun{{0, 5001, sparse.Data}}, digest: "c07f24585606c62c438578e23fe5e876c6865df49240896e4fbf5ccbcd75cc6d"},
		{name: "zero", body: make([]byte, 5001), payload: 5001, zeroEnd: 5001, runs: []expectedRun{{0, 4096, sparse.Zero}, {4096, 5001, sparse.Data}}, digest: "66d670b9c2b0762632d775eb84b7ed144a0af6fa390dd418272ad18f99f478a4"},
		{name: "hole", body: make([]byte, 5001), payload: 5001, holes: []sparse.Extent{{Size: 5001}}, runs: []expectedRun{{0, 5001, sparse.Hole}}, digest: "03daea078e622bc8426437df0a02c25b146f973220b2b4cfcd7307ecec8c4784"},
		{name: "last-hole", body: append(bytes.Repeat([]byte{'b'}, 4096), make([]byte, 905)...), payload: 5001, holes: []sparse.Extent{{Offset: 4096, Size: 905}}, runs: []expectedRun{{0, 4096, sparse.Data}, {4096, 5001, sparse.Hole}}, digest: "d090cc7057e9a8f3a232af1fcf64c22d3603be536137318b96d872bf78cd42bb"},
		{name: "last-hole-with-tail", body: append(append(bytes.Repeat([]byte{'b'}, 4096), make([]byte, 905)...), bytes.Repeat([]byte{'T'}, 17)...), payload: 5001, holes: []sparse.Extent{{Offset: 4096, Size: 905}}, runs: []expectedRun{{0, 4096, sparse.Data}, {4096, 5001, sparse.Hole}, {5001, 5018, sparse.Data}}, digest: "2eeb7b98b1c616655bb22f28a350cca4019666a31bf6c10ca4104cee42a64338"},
		{name: "partial-payload-with-tail", body: []byte("hellotail"), payload: 5, runs: []expectedRun{{0, 9, sparse.Data}}, digest: "f3b811b5aaf9d449e987dfafd11410e57fb9aa3883c587646f63f81974d66499"},
	} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encrypted=%t", tc.name, encrypted), func(t *testing.T) {
				counter := &countingReaderAt{ReaderAt: bytes.NewReader(tc.body)}
				base, err := sparse.NewSource(counter, uint64(len(tc.body)), tc.holes)
				if err != nil {
					t.Fatal(err)
				}
				source := triCarrierSource{base, tc.zeroStart, tc.zeroEnd, tc.payload}
				var output bytes.Buffer
				var writeOptions []WriteOption
				var readOptions []ReadOption
				if encrypted {
					codec := WithCodec(&optionTestCodec{}, true)
					writeOptions = append(writeOptions, codec)
					readOptions = append(readOptions, codec)
				}
				scheme, digest, err := WriteTo(context.Background(), &output, "disk", source, writeOptions...)
				if err != nil || digest != tc.digest {
					t.Fatalf("logical identity = %s, want %s; error=%v", digest, tc.digest, err)
				}
				wantRead := uint64(len(tc.body)) - (tc.zeroEnd - tc.zeroStart)
				for _, hole := range tc.holes {
					wantRead -= hole.Size
				}
				if counter.read != int64(wantRead) {
					t.Fatalf("writer payload reads = %d, want %d", counter.read, wantRead)
				}
				readOptions = append(readOptions, WithExpectedDigest(scheme, digest))
				for _, sequential := range []bool{false, true} {
					var opened sparse.Source
					if sequential {
						opened, _, err = SourceFrom(readerOnly{bytes.NewReader(output.Bytes())}, "disk", readOptions...)
					} else {
						opened, _, err = SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "disk", readOptions...)
					}
					if err != nil {
						t.Fatal(err)
					}
					if opened.Size() != uint64(len(tc.body)) {
						t.Fatalf("logical length was rounded: %d", opened.Size())
					}
					payload, _, _ := opened.(IdentityProvider).PayloadCommitment()
					if payload != tc.payload {
						t.Fatalf("payload length was rounded: %d", payload)
					}
					for _, want := range tc.runs {
						for off := want.start; off < want.end; {
							run, err := opened.RunAt(off, want.end-off)
							if err != nil || run.Kind() != want.kind || run.End() <= off || run.End() > want.end {
								t.Fatal("logical run changed", sequential, off, run, err)
							}
							off = run.End()
						}
					}
					got := make([]byte, len(tc.body))
					if len(got) != 0 {
						if n, err := opened.ReadAt(context.Background(), got, 0); n != len(got) || err != nil || !bytes.Equal(got, tc.body) {
							t.Fatal("logical bytes changed", sequential, n, err)
						}
					}
					if n, err := opened.ReadAt(context.Background(), make([]byte, 1), opened.Size()); n != 0 || !errors.Is(err, io.EOF) {
						t.Fatalf("bytes beyond exact logical EOF: %d, %v", n, err)
					}
				}
			})
		}
	}
}

func TestPresencePartialZeroPreservesBytesAndIdentity(t *testing.T) {
	body := bytes.Repeat([]byte{0xa5}, 4*4096)
	clear(body[:4096])
	const zeroStart, zeroEnd = 4096 + 13, 3*4096 + 17
	clear(body[zeroStart:zeroEnd])
	holes := []sparse.Extent{{Size: 4096}}
	ordinary, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
	if err != nil {
		t.Fatal(err)
	}
	var denseZeros bytes.Buffer
	_, wantDigest, err := WriteTo(context.Background(), &denseZeros, "disk", ordinary)
	if err != nil {
		t.Fatal(err)
	}
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			counter := &countingReaderAt{ReaderAt: bytes.NewReader(body)}
			base, err := sparse.NewSource(counter, uint64(len(body)), holes)
			if err != nil {
				t.Fatal(err)
			}
			source := triCarrierSource{base, zeroStart, zeroEnd, uint64(len(body))}
			var output bytes.Buffer
			var writeOptions []WriteOption
			var readOptions []ReadOption
			if encrypted {
				codec := WithCodec(&optionTestCodec{}, true)
				writeOptions = append(writeOptions, codec)
				readOptions = append(readOptions, codec)
			}
			scheme, digest, err := WriteTo(context.Background(), &output, "disk", source, writeOptions...)
			if err != nil || digest != wantDigest {
				t.Fatalf("Zero representation changed identity: %s, %v", digest, err)
			}
			if counter.read != 13+(4096-17) {
				t.Fatalf("writer read Zero/Hole payload: %d", counter.read)
			}
			if !encrypted && output.Len() != denseZeros.Len()-4096 {
				t.Fatal("writer must omit only the complete Zero page")
			}
			readOptions = append(readOptions, WithExpectedDigest(scheme, digest))
			for _, sequential := range []bool{false, true} {
				var opened sparse.Source
				if sequential {
					opened, _, err = SourceFrom(readerOnly{bytes.NewReader(output.Bytes())}, "disk", readOptions...)
				} else {
					opened, _, err = SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "disk", readOptions...)
				}
				if err != nil {
					t.Fatal(err)
				}
				for i, want := range []sparse.RunKind{sparse.Hole, sparse.Data, sparse.Zero, sparse.Data} {
					run, err := opened.RunAt(uint64(i*4096), 4096)
					if err != nil || run.Kind() != want {
						t.Fatal("page state changed", sequential, i, run, err)
					}
				}
				got := make([]byte, len(body))
				if n, err := opened.ReadAt(context.Background(), got, 0); n != len(got) || err != nil || !bytes.Equal(got, body) {
					t.Fatal("logical bytes changed", sequential, n, err)
				}
			}
		})
	}
}
