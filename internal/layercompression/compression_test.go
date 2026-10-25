package layercompression

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type observedReader struct {
	io.Reader
	closes int
}

func (r *observedReader) Close() error {
	r.closes++
	return nil
}

type shortReader struct {
	r io.Reader
}

func (r shortReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.r.Read(p)
}

type sourceErrorReader struct {
	payload []byte
	err     error
}

func (r *sourceErrorReader) Read(p []byte) (int, error) {
	n := copy(p, r.payload)
	r.payload = r.payload[n:]
	if len(r.payload) == 0 {
		return n, r.err
	}
	return n, nil
}

func testTar(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	payload := bytes.Repeat([]byte("layer payload\n"), 200)
	if err := w.WriteHeader(&tar.Header{Name: "payload", Mode: 0644, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func testEncodings(t *testing.T, plain []byte) (gz, zs, skippable []byte) {
	t.Helper()
	var g bytes.Buffer
	gw := gzip.NewWriter(&g)
	if _, err := gw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	var z bytes.Buffer
	zw, err := zstd.NewWriter(&z, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	metadata := []byte("layer metadata")
	header := zstd.Header{Skippable: true, SkippableID: 3, SkippableSize: uint32(len(metadata))}
	prefix, err := header.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix = append(prefix, metadata...)
	return g.Bytes(), z.Bytes(), append(prefix, z.Bytes()...)
}

func TestOpenLayerContent(t *testing.T) {
	plain := testTar(t)
	gz, zs, skippable := testEncodings(t, plain)
	for _, tc := range []struct {
		name string
		blob []byte
		kind Kind
	}{
		{"plain", plain, Plain},
		{"gzip", gz, Gzip},
		{"zstd", zs, Zstd},
		{"zstd-skippable", skippable, Zstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, segmented := range []bool{false, true} {
				var input io.Reader = bytes.NewReader(tc.blob)
				if segmented {
					input = shortReader{r: input}
				}
				body := &observedReader{Reader: input}
				r, kind, err := Open(body, Options{DecoderConcurrency: 1})
				if err != nil {
					t.Fatalf("segmented=%v: %v", segmented, err)
				}
				got, readErr := io.ReadAll(r)
				closeErr := r.Close()
				if readErr != nil || closeErr != nil {
					t.Fatalf("segmented=%v: read=%v close=%v", segmented, readErr, closeErr)
				}
				if kind != tc.kind || !bytes.Equal(got, plain) {
					t.Fatalf("segmented=%v: kind=%v want=%v, decoded content match=%v", segmented, kind, tc.kind, bytes.Equal(got, plain))
				}
				if body.closes != 0 {
					t.Fatalf("decoder closed caller's body %d times", body.closes)
				}
			}
		})
	}
}

func TestDetectPreservesStoredBytes(t *testing.T) {
	plain := testTar(t)
	gz, zs, skippable := testEncodings(t, plain)
	for _, blob := range [][]byte{plain, gz, zs, skippable} {
		_, r, err := Detect(shortReader{r: bytes.NewReader(blob)})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		if err != nil || !bytes.Equal(got, blob) {
			t.Fatalf("stored bytes changed: err=%v", err)
		}
	}
}

func TestShortUnrecognizedInputPassesThrough(t *testing.T) {
	for _, input := range [][]byte{nil, {'a'}, {'a', 'b'}, {'a', 'b', 'c'}, {0x1f}, {0x28, 0xb5, 0x2f}} {
		r, kind, err := Open(bytes.NewReader(input), Options{})
		if err != nil {
			t.Fatalf("input=%x: %v", input, err)
		}
		got, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || kind != Plain || !bytes.Equal(got, input) {
			t.Fatalf("input=%x: kind=%v output=%x read=%v close=%v", input, kind, got, readErr, closeErr)
		}
	}
}

func TestSourceErrorsArePreserved(t *testing.T) {
	want := errors.New("source read failed")
	for _, payload := range [][]byte{nil, []byte("abc"), []byte("plain bytes after the prefix")} {
		body := &observedReader{Reader: &sourceErrorReader{payload: payload, err: want}}
		r, _, err := Open(body, Options{})
		if err == nil {
			_, err = io.ReadAll(r)
			err = errors.Join(err, r.Close())
		}
		if !errors.Is(err, want) {
			t.Fatalf("length=%d: got %v, want source failure", len(payload), err)
		}
		if body.closes != 0 {
			t.Fatal("error closed caller's body")
		}
	}
}

func TestZstdDecoderOptions(t *testing.T) {
	_, blob, _ := testEncodings(t, testTar(t))
	for _, tc := range []struct {
		name    string
		options Options
	}{
		{"memory-bound", Options{DecoderMemory: 1, DecoderConcurrency: 1}},
		{"invalid-concurrency", Options{DecoderConcurrency: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, kind, err := Open(bytes.NewReader(blob), tc.options)
			if err == nil {
				_, err = io.ReadAll(r)
				err = errors.Join(err, r.Close())
			}
			if err == nil || kind != Zstd {
				t.Fatalf("decoder options were ignored: kind=%v error=%v", kind, err)
			}
		})
	}
}

func TestRecognizedCompressionNeverFallsBack(t *testing.T) {
	gz, zs, _ := testEncodings(t, testTar(t))
	for _, tc := range []struct {
		name string
		blob []byte
		kind Kind
	}{
		{"gzip-magic-only", []byte{0x1f, 0x8b}, Gzip},
		{"gzip-short-header", []byte{0x1f, 0x8b, 8}, Gzip},
		{"gzip-invalid-method", []byte{0x1f, 0x8b, 0, 0, 0, 0, 0, 0, 0, 0}, Gzip},
		{"gzip-truncated", gz[:len(gz)-1], Gzip},
		{"zstd-magic-only", []byte{0x28, 0xb5, 0x2f, 0xfd}, Zstd},
		{"zstd-reserved-bit", []byte{0x28, 0xb5, 0x2f, 0xfd, 8}, Zstd},
		{"zstd-truncated", zs[:len(zs)-1], Zstd},
		{"zstd-skippable-truncated", []byte{0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0}, Zstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedReader{Reader: bytes.NewReader(tc.blob)}
			r, kind, err := Open(body, Options{DecoderConcurrency: 1})
			if err == nil {
				_, err = io.ReadAll(r)
				err = errors.Join(err, r.Close())
			}
			if err == nil || kind != tc.kind {
				t.Fatalf("kind=%v want=%v error=%v", kind, tc.kind, err)
			}
			if body.closes != 0 {
				t.Fatal("invalid input closed caller's body")
			}
		})
	}
}
