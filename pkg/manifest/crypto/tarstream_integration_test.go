package crypto

import (
	archivetar "archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

type streamReaderOnly struct{ io.Reader }

type shortReader struct {
	r   io.Reader
	max int
}

func (r shortReader) Read(buffer []byte) (int, error) {
	if len(buffer) > r.max {
		buffer = buffer[:r.max]
	}
	return r.r.Read(buffer)
}

type shortWriter struct {
	w   io.Writer
	max int
}

func (w shortWriter) Write(buffer []byte) (int, error) {
	if len(buffer) > w.max {
		buffer = buffer[:w.max]
	}
	return w.w.Write(buffer)
}

type shortReaderAt struct {
	r   io.ReaderAt
	max int
}

func (r shortReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	if len(buffer) > r.max {
		buffer = buffer[:r.max]
	}
	return r.r.ReadAt(buffer, offset)
}

type countedReaderAt struct {
	reader io.ReaderAt
	calls  atomic.Int64
	bytes  atomic.Int64
}

func (r *countedReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := r.reader.ReadAt(buffer, offset)
	r.calls.Add(1)
	r.bytes.Add(int64(n))
	return n, err
}

func (r *countedReaderAt) reset() {
	r.calls.Store(0)
	r.bytes.Store(0)
}

func TestEncryptedTarStreamWireStructureAndRandomization(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	codec, _ := NewTarStreamCodec(key)
	artifact, scheme, digest := writeTarArtifact(t, codec, "golden", []byte("hello"), nil)
	again, againScheme, againDigest := writeTarArtifact(t, codec, "golden", []byte("hello"), nil)
	if len(artifact) != 4259 {
		t.Fatalf("artifact length = %d, want 4259", len(artifact))
	}
	if !bytes.Equal(artifact[:8], []byte{0x89, 'K', 'T', 'S', 'E', 'N', 'C', '\n'}) ||
		binary.BigEndian.Uint16(artifact[8:10]) != 1 || binary.BigEndian.Uint16(artifact[10:12]) != 48 ||
		binary.BigEndian.Uint32(artifact[12:16]) != 0 {
		t.Fatalf("invalid clear prefix: %x", artifact[:48])
	}
	if bytes.Equal(artifact[16:48], make([]byte, 32)) || bytes.Equal(artifact[16:48], again[16:48]) || bytes.Equal(artifact, again) {
		t.Fatal("artifact salt and physical ciphertext must be randomized")
	}
	if scheme != tarstream.DigestSchemeHMAC || againScheme != scheme || againDigest != digest {
		t.Fatalf("logical identities = %s:%s and %s:%s", scheme, digest, againScheme, againDigest)
	}
	for _, value := range [][]byte{artifact, again} {
		source, _, err := tarstream.SourceAt(bytes.NewReader(value), int64(len(value)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 5)
		if _, err := source.ReadAt(context.Background(), got, 0); err != nil || string(got) != "hello" {
			t.Fatalf("randomized artifact read = %q, %v", got, err)
		}
	}
}

func writeTarArtifact(t testing.TB, codec tarstream.Codec, name string, body []byte, holes []sparse.Extent) ([]byte, string, string) {
	t.Helper()
	source, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	options := []tarstream.WriteOption(nil)
	if codec != nil {
		options = append(options, tarstream.WithCodec(codec, false))
	}
	scheme, digest, err := tarstream.WriteTo(context.Background(), &output, name, source, options...)
	if err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), scheme, digest
}

func firstPayloadRecord(t testing.TB, name string, body []byte, holes []sparse.Extent) int {
	t.Helper()
	plain, _, _ := writeTarArtifact(t, nil, name, body, holes)
	r := bytes.NewReader(plain)
	if _, err := tarstream.ReadSeekFrom(r, name); err != nil {
		t.Fatal(err)
	}
	off, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	// Clear prefix + sealed envelope header + plaintext metadata and one
	// authentication overhead per preceding metadata record.
	return 48 + 81 + int(off) + int((off+4095)/4096)*17
}

func readSparseSource(ctx context.Context, source sparse.Source) ([]byte, error) {
	result := make([]byte, source.Size())
	for offset := uint64(0); offset < source.Size(); {
		run, err := source.RunAt(offset, source.Size()-offset)
		if err != nil {
			return nil, err
		}
		if run.End() <= offset || run.End() > source.Size() {
			return nil, fmt.Errorf("invalid run [%d,%d)", offset, run.End())
		}
		if run.Kind() == sparse.Hole {
			offset = run.End()
			continue
		}
		for offset < run.End() {
			length := min(uint64(4096), run.End()-offset)
			n, err := run.ReadAt(ctx, result[offset:offset+length], offset-run.Offset())
			offset += uint64(n)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				return nil, io.ErrNoProgress
			}
		}
	}
	return result, nil
}

func TestEncryptedTarStreamRoundTripAndIdentity(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	codec, _ := NewTarStreamCodec(key)
	body := make([]byte, 3<<20)
	copy(body, bytes.Repeat([]byte("A"), 8192))
	copy(body[1<<20:], bytes.Repeat([]byte("B"), 4096))
	holes := []sparse.Extent{{Offset: 8192, Size: (1 << 20) - 8192}, {Offset: (1 << 20) + 4096, Size: (2 << 20) - 4096}}

	encrypted, scheme, digest := writeTarArtifact(t, codec, "image", body, holes)
	again, againScheme, againDigest := writeTarArtifact(t, codec, "image", body, holes)
	if scheme != tarstream.DigestSchemeHMAC || againScheme != scheme || againDigest != digest {
		t.Fatalf("encrypted digests = %s:%s and %s:%s", scheme, digest, againScheme, againDigest)
	}
	if bytes.Equal(encrypted, again) {
		t.Fatal("same key and plaintext produced identical encrypted bytes")
	}
	requiredSource, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
	if err != nil {
		t.Fatal(err)
	}
	var requiredOutput bytes.Buffer
	requiredScheme, requiredDigest, err := tarstream.WriteTo(context.Background(), &requiredOutput, "image", requiredSource, tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	if requiredScheme != scheme || requiredDigest != digest || bytes.Equal(requiredOutput.Bytes(), encrypted) {
		t.Fatal("required mode changed identity or failed to randomize physical output")
	}

	plaintext, plainScheme, plainDigest := writeTarArtifact(t, nil, "image", body, holes)
	if plainScheme != tarstream.DigestScheme {
		t.Fatalf("plaintext scheme = %q", plainScheme)
	}
	plainSource, _, err := tarstream.SourceAt(bytes.NewReader(plaintext), int64(len(plaintext)), "", tarstream.WithCodec(codec, false))
	if err != nil {
		t.Fatal(err)
	}
	plainDigester := plainSource.(tarstream.Digester)
	gotScheme, gotDigest := plainDigester.Digest()
	if gotScheme != plainScheme || gotDigest != plainDigest || gotDigest == digest {
		t.Fatalf("plaintext carrier identity = %s:%s, want %s:%s and distinct from encrypted %s", gotScheme, gotDigest, plainScheme, plainDigest, digest)
	}
	plainSequential, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(plaintext)}, "", tarstream.WithCodec(codec, false), tarstream.WithExpectedDigest(plainScheme, plainDigest))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readSparseSource(context.Background(), plainSequential); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("key-bound plaintext sequential read: %v", err)
	}

	random, name, err := tarstream.SourceAt(bytes.NewReader(encrypted), int64(len(encrypted)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	if name != "image" || random.Size() != uint64(len(body)) {
		t.Fatalf("random source = %q/%d", name, random.Size())
	}
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			offset := uint64((i * 7919) % (len(body) - 4096))
			buffer := make([]byte, 4096)
			if _, err := random.ReadAt(context.Background(), buffer, offset); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(buffer, body[offset:offset+4096]) {
				errs <- fmt.Errorf("random mismatch at %d", offset)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	sequential, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(encrypted)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSparseSource(context.Background(), sequential)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("sequential plaintext mismatch")
	}

	if _, _, err := tarstream.SourceAt(bytes.NewReader(plaintext), int64(len(plaintext)), "", tarstream.WithCodec(codec, true)); !errors.Is(err, tarstream.ErrPlaintextForbidden) {
		t.Fatalf("required plaintext error = %v", err)
	}
	if _, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(plaintext)}, "", tarstream.WithCodec(codec, true)); !errors.Is(err, tarstream.ErrPlaintextForbidden) {
		t.Fatalf("required sequential plaintext error = %v", err)
	}
	if _, _, err := tarstream.SourceAt(bytes.NewReader(encrypted), int64(len(encrypted)), ""); !errors.Is(err, tarstream.ErrCodecRequired) {
		t.Fatalf("encrypted without codec error = %v", err)
	}
	wrongExpected := bytes.Repeat([]byte{'0'}, 64)
	if _, _, err := tarstream.SourceAt(bytes.NewReader(encrypted), int64(len(encrypted)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, string(wrongExpected))); !errors.Is(err, tarstream.ErrDigestMismatch) {
		t.Fatalf("wrong expected digest error = %v", err)
	}

	other, _ := NewTarStreamCodec([32]byte{0xff})
	_, otherScheme, otherDigest := writeTarArtifact(t, other, "image", body, holes)
	if otherScheme != scheme || otherDigest == digest {
		t.Fatalf("different-key identity = %s:%s, original %s:%s", otherScheme, otherDigest, scheme, digest)
	}
}

func TestEncryptedTarStreamTamperAndBounds(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{7})
	body := bytes.Repeat([]byte("data"), 4096)
	artifact, scheme, digest := writeTarArtifact(t, codec, "image", body, nil)
	open := func(value []byte, selected tarstream.Codec) (sparse.Source, error) {
		source, _, err := tarstream.SourceAt(bytes.NewReader(value), int64(len(value)), "", tarstream.WithCodec(selected, true), tarstream.WithExpectedDigest(scheme, digest))
		return source, err
	}

	wrong, _ := NewTarStreamCodec([32]byte{8})
	if _, err := open(artifact, wrong); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("wrong-key error = %v", err)
	}
	version := append([]byte(nil), artifact...)
	version[9] = 2
	if _, err := open(version, codec); !errors.Is(err, tarstream.ErrUnsupportedVersion) {
		t.Fatalf("version error = %v", err)
	}
	flags := append([]byte(nil), artifact...)
	flags[15] = 1
	if _, err := open(flags, codec); !errors.Is(err, tarstream.ErrMalformedEnvelope) {
		t.Fatalf("flags error = %v", err)
	}
	oldPrefixGeometry := append([]byte(nil), artifact...)
	binary.BigEndian.PutUint16(oldPrefixGeometry[10:12], 16)
	if _, err := open(oldPrefixGeometry, codec); !errors.Is(err, tarstream.ErrMalformedEnvelope) {
		t.Fatalf("old prefix geometry error = %v", err)
	}
	salt := append([]byte(nil), artifact...)
	salt[20] ^= 0x01
	if _, err := open(salt, codec); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("salt tamper error = %v", err)
	}
	header := append([]byte(nil), artifact...)
	header[49] ^= 0x01
	if _, err := open(header, codec); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("header tamper error = %v", err)
	}
	suffix := append([]byte(nil), artifact...)
	suffix[len(suffix)-10] ^= 0x01
	if _, err := open(suffix, codec); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("suffix tamper error = %v", err)
	}
	if _, err := open(artifact[:len(artifact)-1], codec); !errors.Is(err, tarstream.ErrMalformedEnvelope) {
		t.Fatalf("truncation error = %v", err)
	}
	if _, err := open(append(append([]byte(nil), artifact...), 0), codec); !errors.Is(err, tarstream.ErrMalformedEnvelope) {
		t.Fatalf("append error = %v", err)
	}

	// Derive the payload boundary from the carrier, including its presence
	// entry. Metadata-only open must not authenticate a damaged data record.
	data := append([]byte(nil), artifact...)
	firstDataRecord := firstPayloadRecord(t, "image", body, nil)
	data[firstDataRecord+1] ^= 0x01
	source, err := open(data, codec)
	if err != nil {
		t.Fatalf("fast open authenticated an untouched suffix: %v", err)
	}
	buffer := make([]byte, 4096)
	if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("data tamper read error = %v", err)
	}

	// A coalesced random read exposes only records that authenticated. A later
	// record failure preserves the authenticated prefix and leaves the failing
	// record's destination untouched.
	const fullRecord = 4096 + 17
	later := append([]byte(nil), artifact...)
	later[firstDataRecord+fullRecord+1] ^= 0x01
	source, err = open(later, codec)
	if err != nil {
		t.Fatal(err)
	}
	batch := bytes.Repeat([]byte{0xa5}, 8192)
	// sparse.Source has an exact-fill contract and reports zero for the failed
	// logical read even though its ReaderAt filled the authenticated prefix.
	if n, err := source.ReadAt(context.Background(), batch, 0); n != 0 || !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("later record tamper read = %d, %v", n, err)
	}
	if !bytes.Equal(batch[:4096], body[:4096]) || !bytes.Equal(batch[4096:], bytes.Repeat([]byte{0xa5}, 4096)) {
		t.Fatal("coalesced authentication failure exposed unauthenticated plaintext")
	}
	for _, tc := range []struct {
		name   string
		offset int
	}{
		{"record-flag", firstDataRecord},
		{"record-tag", firstDataRecord + 4096 + 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tampered := append([]byte(nil), artifact...)
			tampered[tc.offset] ^= 1
			source, err := open(tampered, codec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, tarstream.ErrAuthentication) {
				t.Fatalf("read error = %v", err)
			}
		})
	}
}

func TestEncryptedTarStreamSplicingBoundary(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{3})
	aBody := bytes.Repeat([]byte{'A'}, 8192)
	bBody := bytes.Repeat([]byte{'B'}, 8192)
	a, scheme, digest := writeTarArtifact(t, codec, "image", aBody, nil)
	b, _, _ := writeTarArtifact(t, codec, "image", bBody, nil)

	firstDataRecord := firstPayloadRecord(t, "image", aBody, nil)
	const fullRecord = 4096 + 17
	spliced := append([]byte(nil), a...)
	copy(spliced[firstDataRecord:firstDataRecord+fullRecord], b[firstDataRecord:firstDataRecord+fullRecord])
	random, _, err := tarstream.SourceAt(bytes.NewReader(spliced), int64(len(spliced)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatalf("fast validation rejected same-layout splice before access: %v", err)
	}
	buffer := make([]byte, 4096)
	if _, err := random.ReadAt(context.Background(), buffer, 0); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("spliced record read error = %v", err)
	}

	sequential, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(spliced)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readSparseSource(context.Background(), sequential); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("full validation splice error = %v", err)
	}
}

func TestEncryptedTarStreamAllHoleFinalizesAtOpen(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{4})
	body := make([]byte, 8192)
	artifact, scheme, digest := writeTarArtifact(t, codec, "image", body, []sparse.Extent{{Offset: 0, Size: uint64(len(body))}})
	source, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(artifact)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	if source.Size() != uint64(len(body)) {
		t.Fatalf("all-hole size = %d", source.Size())
	}
	empty, emptyScheme, emptyDigest := writeTarArtifact(t, codec, "empty", nil, nil)
	emptySource, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(empty)}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(emptyScheme, emptyDigest))
	if err != nil {
		t.Fatal(err)
	}
	if emptySource.Size() != 0 {
		t.Fatalf("empty source size = %d", emptySource.Size())
	}

	tampered := append([]byte(nil), artifact...)
	tampered[len(tampered)-1] ^= 1
	if _, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(tampered)}, "", tarstream.WithCodec(codec, true)); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("all-hole suffix tamper error = %v", err)
	}
}

func TestEncryptedTarStreamMarkerCrossesRecords(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{5})
	// Dense prefix 1536 + padded payload 2048 puts the marker at plaintext
	// offset 3584, so the fixed 1536-byte marker/trailer suffix crosses a 4 KiB
	// record boundary.
	body := bytes.Repeat([]byte{0x5a}, 2048)
	artifact, scheme, digest := writeTarArtifact(t, codec, "image", body, nil)
	source, _, err := tarstream.SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(body))
	if _, err := source.ReadAt(context.Background(), got, 0); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("cross-record marker round trip: %v", err)
	}
}

func TestEncryptedTarStreamBoundedPhysicalIO(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{6})
	body := bytes.Repeat([]byte{0x6a}, 2<<20)
	artifact, _, _ := writeTarArtifact(t, codec, "image", body, nil)
	reader := &countedReaderAt{reader: bytes.NewReader(artifact)}
	source, _, err := tarstream.SourceAt(reader, int64(len(artifact)), "", tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	if read := reader.bytes.Load(); read >= int64(len(body))/100 {
		t.Fatalf("encrypted fast open read %d bytes for a %d-byte payload", read, len(body))
	}
	reader.reset()
	block := make([]byte, 4096)
	if _, err := source.ReadAt(context.Background(), block, 4096); err != nil {
		t.Fatal(err)
	}
	if calls, read := reader.calls.Load(), reader.bytes.Load(); calls != 1 || read != 4096+17 {
		t.Fatalf("aligned 4 KiB miss used %d calls/%d bytes, want 1/%d", calls, read, 4096+17)
	}
	if _, err := source.ReadAt(context.Background(), block, 4096); err != nil {
		t.Fatal(err)
	}
	if calls, read := reader.calls.Load(), reader.bytes.Load(); calls != 2 || read != 2*(4096+17) {
		t.Fatalf("repeated 4 KiB read used %d calls/%d bytes, want 2/%d", calls, read, 2*(4096+17))
	}

	reader = &countedReaderAt{reader: bytes.NewReader(artifact)}
	source, _, err = tarstream.SourceAt(reader, int64(len(artifact)), "", tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	reader.reset()
	continuous := make([]byte, 1<<20)
	if _, err := source.ReadAt(context.Background(), continuous, 0); err != nil {
		t.Fatal(err)
	}
	if calls := reader.calls.Load(); calls != 2 {
		t.Fatalf("1 MiB continuous miss used %d underlying reads, want 2 bounded coalesce batches", calls)
	}
	if !bytes.Equal(continuous, body[:len(continuous)]) {
		t.Fatal("continuous read mismatch")
	}
}

func TestEncryptedTarStreamShortIO(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{10})
	body := bytes.Repeat([]byte("short-io"), 2048)
	source := sparse.Dense(bytes.NewReader(body), uint64(len(body)))
	var output bytes.Buffer
	scheme, digest, err := tarstream.WriteTo(context.Background(), shortWriter{w: &output, max: 7}, "image", source, tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	sequential, _, err := tarstream.SourceFrom(streamReaderOnly{shortReader{r: bytes.NewReader(output.Bytes()), max: 1}}, "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSparseSource(context.Background(), sequential)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("short-I/O round trip mismatch")
	}

	_, _, err = tarstream.SourceAt(
		shortReaderAt{r: bytes.NewReader(output.Bytes()), max: 3},
		int64(output.Len()),
		"",
		tarstream.WithCodec(codec, true),
		tarstream.WithExpectedDigest(scheme, digest),
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short nil ReaderAt must not splice attempts: %v", err)
	}
}

func TestEncryptedTarStreamRecordSwapRejected(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{11})
	body := bytes.Repeat([]byte("0123456789abcdef"), 1024)
	artifact, _, _ := writeTarArtifact(t, codec, "image", body, nil)
	firstDataRecord := firstPayloadRecord(t, "image", body, nil)
	const fullRecord = 4096 + 17
	swapped := append([]byte(nil), artifact...)
	first := append([]byte(nil), swapped[firstDataRecord:firstDataRecord+fullRecord]...)
	second := append([]byte(nil), swapped[firstDataRecord+fullRecord:firstDataRecord+2*fullRecord]...)
	copy(swapped[firstDataRecord:firstDataRecord+fullRecord], second)
	copy(swapped[firstDataRecord+fullRecord:firstDataRecord+2*fullRecord], first)
	source, _, err := tarstream.SourceAt(bytes.NewReader(swapped), int64(len(swapped)), "", tarstream.WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadAt(context.Background(), make([]byte, 4096), 0); !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("record swap read error = %v", err)
	}
}

func TestEncryptedSequentialAuthenticationFailureIsSticky(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{16})
	body := bytes.Repeat([]byte("0123456789abcdef"), 512)
	artifact, scheme, digest := writeTarArtifact(t, codec, "image", body, nil)
	firstDataRecord := firstPayloadRecord(t, "image", body, nil)
	const fullRecord = 4096 + 17
	artifact[firstDataRecord+fullRecord+1] ^= 1
	source, _, err := tarstream.SourceFrom(
		streamReaderOnly{bytes.NewReader(artifact)},
		"",
		tarstream.WithCodec(codec, true),
		tarstream.WithExpectedDigest(scheme, digest),
	)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(body))
	if n, err := source.ReadAt(context.Background(), buffer, 0); n != 4096 || !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("cross-record read = %d, %v; want 4096, authentication failure", n, err)
	}
	if n, err := source.ReadAt(context.Background(), buffer[:4096], 4096); n != 0 || !errors.Is(err, tarstream.ErrAuthentication) {
		t.Fatalf("retry after authentication failure = %d, %v", n, err)
	}
}

func TestEncryptedTarStreamDoesNotExposePlainDigest(t *testing.T) {
	codec, _ := NewTarStreamCodec([32]byte{12})
	body := bytes.Repeat([]byte("identity"), 1024)
	plaintext, _, plainDigest := writeTarArtifact(t, nil, "image", body, nil)
	encrypted, scheme, digest := writeTarArtifact(t, codec, "image", body, nil)
	if scheme != tarstream.DigestSchemeHMAC || digest == plainDigest {
		t.Fatalf("key-bound identity = %s:%s, plain digest %s", scheme, digest, plainDigest)
	}
	if bytes.Contains(encrypted, []byte(tarstream.DigestMarkerPrefix)) || bytes.Contains(encrypted, []byte(plainDigest)) {
		t.Fatal("encrypted bytes expose the inner marker or plain digest")
	}
	if !bytes.Contains(plaintext, []byte(plainDigest)) {
		t.Fatal("plaintext control does not contain its marker digest")
	}
	wrong := string(bytes.Repeat([]byte{'0'}, 64))
	_, _, err := tarstream.SourceAt(bytes.NewReader(encrypted), int64(len(encrypted)), "", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest(scheme, wrong))
	if !errors.Is(err, tarstream.ErrDigestMismatch) {
		t.Fatalf("expected mismatch error = %v", err)
	}
	if strings.Contains(err.Error(), plainDigest) {
		t.Fatal("key-bound error exposes plain digest")
	}
}

func TestKeyBoundParserErrorsDoNotExposePlaintextMetadata(t *testing.T) {
	const secretName = "customer-secret-entry-name"
	var artifact bytes.Buffer
	w := archivetar.NewWriter(&artifact)
	if err := w.WriteHeader(&archivetar.Header{Name: secretName, Mode: 0o755, Typeflag: archivetar.TypeDir}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	codec, _ := NewTarStreamCodec([32]byte{15})
	for _, open := range []func() error{
		func() error {
			_, _, err := tarstream.SourceAt(bytes.NewReader(artifact.Bytes()), int64(artifact.Len()), secretName, tarstream.WithCodec(codec, false))
			return err
		},
		func() error {
			_, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(artifact.Bytes())}, secretName, tarstream.WithCodec(codec, false))
			return err
		},
	} {
		err := open()
		if !errors.Is(err, tarstream.ErrInvalidCanonicalTarstream) {
			t.Fatalf("key-bound parser error = %v", err)
		}
		if strings.Contains(err.Error(), secretName) {
			t.Fatalf("key-bound parser error exposed plaintext metadata: %v", err)
		}
	}
}

func FuzzEncryptedTarStreamReaders(f *testing.F) {
	codec, _ := NewTarStreamCodec([32]byte{0x24})
	var valid bytes.Buffer
	_, _, err := tarstream.WriteTo(
		context.Background(), &valid, "image",
		sparse.Dense(bytes.NewReader([]byte("seed payload")), uint64(len("seed payload"))),
		tarstream.WithCodec(codec, true),
	)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid.Bytes())
	f.Add([]byte("not an artifact"))
	f.Fuzz(func(t *testing.T, artifact []byte) {
		if len(artifact) > 1<<20 {
			t.Skip()
		}
		if source, _, err := tarstream.SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", tarstream.WithCodec(codec, false)); err == nil {
			buffer := make([]byte, min(source.Size(), uint64(8192)))
			_, _ = source.ReadAt(context.Background(), buffer, 0)
		}
		if source, _, err := tarstream.SourceFrom(streamReaderOnly{bytes.NewReader(artifact)}, "", tarstream.WithCodec(codec, false)); err == nil {
			if source.Size() <= 1<<20 {
				_, _ = readSparseSource(context.Background(), source)
			}
		}
	})
}
