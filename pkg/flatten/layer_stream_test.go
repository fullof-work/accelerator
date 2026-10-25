package flatten

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func layerHash(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

type observedBody struct {
	io.Reader
	closed   atomic.Int32
	closeErr error
}

func (b *observedBody) Close() error { b.closed.Add(1); return b.closeErr }

func encodedLayer(t *testing.T, codec string, plain []byte) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	switch codec {
	case "gzip":
		w := gzip.NewWriter(&b)
		if _, err := w.Write(plain); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes(), "application/vnd.oci.image.layer.v1.tar+gzip"
	case "zstd":
		w, err := zstd.NewWriter(&b, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(plain); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes(), "application/vnd.oci.image.layer.v1.tar+zstd"
	default:
		return bytes.Clone(plain), "application/vnd.oci.image.layer.v1.tar"
	}
}

func streamInput(blob, plain []byte, media string, body *observedBody) LayerInput {
	return LayerInput{MediaType: media, Size: int64(len(blob)), Digest: layerHash(blob), DiffID: layerHash(plain),
		Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
}

func TestLayerCompleteStream(t *testing.T) {
	// tar.Reader stops before the final padding. DiffID covers that padding,
	// not just the entries or the first tar EOF; gzip/zstd trailers follow it.
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "payload", Mode: 0644, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	archive.Write(make([]byte, 8192))
	plain := archive.Bytes()
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			blob, media := encodedLayer(t, codec, plain)
			body := &observedBody{Reader: bytes.NewReader(blob)}
			s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
			if err != nil {
				t.Fatal(err)
			}
			tr := tar.NewReader(s)
			if _, err := tr.Next(); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, tr); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Next(); err != io.EOF {
				t.Fatalf("tar EOF: %v", err)
			}
			if s.decoded.n >= int64(len(plain)) {
				t.Fatal("test did not leave unread tar padding")
			}
			if err := s.finish(); err != nil {
				t.Fatal(err)
			}
			if s.decoded.n != int64(len(plain)) {
				t.Fatal("incomplete DiffID coverage")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if body.closed.Load() != 1 {
				t.Fatalf("Close count %d", body.closed.Load())
			}
		})
	}
}

func TestLayerValidationFailures(t *testing.T) {
	plain := bytes.Repeat([]byte("payload"), 2000)
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		for _, fault := range []string{"size-small", "size-large", "digest", "diffid", "decoded-limit", "close", "truncated"} {
			t.Run(codec+"/"+fault, func(t *testing.T) {
				blob, media := encodedLayer(t, codec, plain)
				// Content-based decoding must still enforce every identity and
				// limit when the descriptor mislabels the stored encoding.
				if codec == "plain" {
					media = "application/vnd.oci.image.layer.v1.tar+gzip"
				} else {
					media = "application/vnd.oci.image.layer.v1.tar"
				}
				body := &observedBody{Reader: bytes.NewReader(blob)}
				in := streamInput(blob, plain, media, body)
				limits := LayerLimits{}
				switch fault {
				case "size-small":
					in.Size--
				case "size-large":
					in.Size++
				case "digest":
					in.Digest = layerHash(nil)
				case "diffid":
					in.DiffID = layerHash(nil)
				case "decoded-limit":
					limits.DecodedBytes = int64(len(plain) - 1)
				case "close":
					body.closeErr = errors.New("owned Close failed")
				case "truncated":
					body.Reader = bytes.NewReader(blob[:len(blob)-1])
				}
				s, err := openLayer(context.Background(), in, limits)
				if err == nil {
					err = s.finish()
				}
				if err == nil {
					t.Fatal("accepted invalid layer")
				}
				if body.closed.Load() != 1 {
					t.Fatalf("Close count %d", body.closed.Load())
				}
			})
		}
	}
}

func TestLayerContentOverridesMediaType(t *testing.T) {
	plain := bytes.Repeat([]byte("layer payload"), 2000)
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		blob, _ := encodedLayer(t, codec, plain)
		for _, media := range []string{
			"application/vnd.oci.image.layer.v1.tar",
			"application/vnd.oci.image.layer.v1.tar+gzip",
			"application/vnd.oci.image.layer.v1.tar+zstd",
			"application/vnd.docker.image.rootfs.diff.tar.gzip",
		} {
			t.Run(codec+"/"+media, func(t *testing.T) {
				body := &observedBody{Reader: bytes.NewReader(blob)}
				s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				got, err := io.ReadAll(s)
				if err != nil || !bytes.Equal(got, plain) {
					t.Fatalf("decoded content differs: %v", err)
				}
				if err := s.finish(); err != nil {
					t.Fatal(err)
				}
				if s.compressed.n != int64(len(blob)) || s.decoded.n != int64(len(plain)) {
					t.Fatal("incomplete stored or decoded byte coverage")
				}
				if body.closed.Load() != 1 {
					t.Fatal("owned body was not closed exactly once")
				}
			})
		}
	}
}

func TestLayerCorruptTrailer(t *testing.T) {
	plain := make([]byte, 2048)
	for _, codec := range []string{"gzip", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			blob, media := encodedLayer(t, codec, plain)
			blob[len(blob)-1] ^= 1 // valid compressed digest, invalid decoder trailer
			body := &observedBody{Reader: bytes.NewReader(blob)}
			s, err := openLayer(context.Background(), streamInput(blob, plain, media, body), LayerLimits{})
			if err == nil {
				err = s.finish()
			}
			if err == nil {
				t.Fatal("accepted corrupt trailer")
			}
		})
	}
}

func TestLayerUnsupportedMediaType(t *testing.T) {
	opened := false
	in := LayerInput{MediaType: "unknown", Size: 0, Digest: layerHash(nil), DiffID: layerHash(nil), Open: func(context.Context) (io.ReadCloser, error) {
		opened = true
		return io.NopCloser(strings.NewReader("")), nil
	}}
	if _, err := openLayer(context.Background(), in, LayerLimits{}); err == nil || opened {
		t.Fatalf("unknown type: err=%v opened=%v", err, opened)
	}
}

type blockedLayerBody struct {
	entered, closed chan struct{}
	prefix          []byte
	once            sync.Once
	closes          atomic.Int32
}

func (b *blockedLayerBody) Read(p []byte) (int, error) {
	if len(b.prefix) != 0 {
		n := copy(p, b.prefix)
		b.prefix = b.prefix[n:]
		return n, nil
	}
	b.once.Do(func() { close(b.entered) })
	<-b.closed
	return 0, io.ErrClosedPipe
}
func (b *blockedLayerBody) Close() error {
	if b.closes.Add(1) == 1 {
		close(b.closed)
	}
	return nil
}

func TestLayerCancellationClosesOwnedBody(t *testing.T) {
	for _, phase := range []string{"sniff", "content"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &blockedLayerBody{entered: make(chan struct{}), closed: make(chan struct{})}
			if phase == "content" {
				body.prefix = []byte("plain layer prefix")
			}
			in := LayerInput{MediaType: "application/vnd.oci.image.layer.v1.tar", Size: 1024, Digest: layerHash(nil), DiffID: layerHash(nil), Open: func(context.Context) (io.ReadCloser, error) { return body, nil }}
			done := make(chan error, 1)
			go func() {
				s, err := openLayer(ctx, in, LayerLimits{})
				if err == nil {
					err = s.finish()
				}
				done <- err
			}()
			select {
			case <-body.entered:
			case err := <-done:
				t.Fatalf("layer did not wait for cancellation: %v", err)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancel left a reader running")
			}
			if body.closes.Load() != 1 {
				t.Fatal("owned body was not closed exactly once")
			}
		})
	}
}
