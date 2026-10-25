package remote

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/compress/zstd"
)

// startRegistry spins up an in-memory OCI registry over httptest and returns
// its host:port (HTTP, hence insecure).
func startRegistry(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func pushImage(t *testing.T, ref string, img v1.Image) {
	t.Helper()
	r, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := ggcrremote.Write(r, img, ggcrremote.WithContext(context.Background())); err != nil {
		t.Fatalf("push %s: %v", ref, err)
	}
}

func testConfig(t *testing.T) *Config {
	t.Helper()
	c := &Config{Insecure: true, Cache: CacheConfig{Dir: t.TempDir()}}
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPullJobsDefaultForDirectConfig(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(128, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/default-jobs:v1"
	pushImage(t, ref, img)

	c := testConfig(t)
	c.PullJobs = 0 // A caller may construct or modify Config without normalize.
	res, err := c.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatalf("Resolve with zero jobs: %v", err)
	}
	cache, err := OpenCache(c.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(context.Background(), res, cache); err != nil {
		t.Fatalf("Pull with zero jobs: %v", err)
	}
}

// TestPullSourceMatches — Resolve pins the right digest, and Pull yields a
// flatten.Source whose config and uncompressed layers match the pushed image,
// with every blob landed in the cache.
func TestPullSourceMatches(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(2048, 3)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/app:v1"
	pushImage(t, ref, img)

	cfg := testConfig(t)
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wantDigest, _ := img.Digest()
	if res.Hash != wantDigest {
		t.Fatalf("resolved %s, want %s", res.Hash, wantDigest)
	}

	cache, err := OpenCache(cfg.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	wantCfg, _ := img.RawConfigFile()
	gotCfg, err := src.ConfigJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCfg, wantCfg) {
		t.Fatal("config JSON mismatch")
	}

	openers, err := src.Layers()
	if err != nil {
		t.Fatal(err)
	}
	imgLayers, _ := img.Layers()
	if len(openers) != len(imgLayers) {
		t.Fatalf("got %d layers, want %d", len(openers), len(imgLayers))
	}
	for i, open := range openers {
		rc, err := open()
		if err != nil {
			t.Fatalf("layer %d open: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		wrc, _ := imgLayers[i].Uncompressed()
		want, _ := io.ReadAll(wrc)
		wrc.Close()
		if !bytes.Equal(got, want) {
			t.Fatalf("layer %d uncompressed content mismatch", i)
		}
	}

	mfst, _ := img.Manifest()
	if !cache.Has(mfst.Config.Digest) {
		t.Fatal("config blob not cached")
	}
	for _, l := range mfst.Layers {
		if !cache.Has(l.Digest) {
			t.Fatalf("layer %s not cached", l.Digest)
		}
	}

	// A second Pull is served entirely from cache (blobs already present).
	if _, err := cfg.Pull(ctx, res, cache); err != nil {
		t.Fatalf("second Pull: %v", err)
	}
}

// TestDecompress — gzip / zstd / plain layer streams all yield the original
// uncompressed tar bytes. Selection is by content sniffing, so a stream that
// contradicts any mediaType labeling still round-trips: gzipped bytes are
// decompressed, plain bytes pass through.
func TestDecompress(t *testing.T) {
	payload := []byte("the quick brown fox jumps over the lazy dog")

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(payload)
	gw.Close()

	var zs bytes.Buffer
	zw, _ := zstd.NewWriter(&zs)
	zw.Write(payload)
	zw.Close()

	cases := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"gzip", gz.Bytes(), payload},
		{"zstd", zs.Bytes(), payload},
		{"plain (mislabeled as gzip in the manifest)", payload, payload},
		{"empty", nil, nil},
	}
	for _, tc := range cases {
		rc, err := decompress(io.NopCloser(bytes.NewReader(tc.in)), v1.Hash{})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("%s: read: %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestDecompressShortInputPassthrough — short streams without a complete
// compression magic pass through as plain rather than enter a decompressor.
func TestDecompressShortInputPassthrough(t *testing.T) {
	for _, in := range [][]byte{nil, {'a'}, {'a', 'b'}, {'a', 'b', 'c'}} {
		rc, err := decompress(io.NopCloser(bytes.NewReader(in)), v1.Hash{})
		if err != nil {
			t.Fatalf("%d-byte input: %v", len(in), err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("%d-byte input: read: %v", len(in), err)
		}
		if !bytes.Equal(got, in) {
			t.Fatalf("%d-byte input: got %q, want plain passthrough %q", len(in), got, in)
		}
	}
}

// TestLooksLikeReference — the export source detector accepts docker refs and
// rejects obvious non-refs.
func TestLooksLikeReference(t *testing.T) {
	for _, s := range []string{"nginx", "nginx:1.27", "gcr.io/p/x@sha256:" + strings.Repeat("a", 64), "registry.example.com:5000/team/app:v1"} {
		if !LooksLikeReference(s) {
			t.Errorf("LooksLikeReference(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "UPPER/CASE:Bad", "bad ref with spaces"} {
		if LooksLikeReference(s) {
			t.Errorf("LooksLikeReference(%q) = true, want false", s)
		}
	}
}

// mislabeledPlainLayer is a v1.Layer whose stored blob is a plain
// (uncompressed) tar while its manifest mediaType claims gzip — the shape
// pushed by tooling that mislabels layers. Every method reports the truth
// about the bytes except MediaType().
type mislabeledPlainLayer struct {
	blob []byte
	d    v1.Hash
}

func (l *mislabeledPlainLayer) Compressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.blob)), nil
}

func (l *mislabeledPlainLayer) Uncompressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.blob)), nil
}

func (l *mislabeledPlainLayer) Digest() (v1.Hash, error) { return l.d, nil }

func (l *mislabeledPlainLayer) DiffID() (v1.Hash, error) { return l.d, nil }

func (l *mislabeledPlainLayer) Size() (int64, error) { return int64(len(l.blob)), nil }

func (l *mislabeledPlainLayer) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}

// plainTarLayer builds a minimal valid plain tar containing one file.
func plainTarLayer(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{Name: "hello.txt", Mode: 0o644, Size: 5, Uid: os.Getuid(), Gid: os.Getgid()}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestPullMislabeledPlainLayer — repro for a pushed image whose layer
// descriptor says gzip but whose blob is a plain tar. Digest verification
// passes (it hashes the stored bytes), so any failure surfaces at layer-open
// time. docker (archive.DecompressStream) and ggcr's own
// layer.Uncompressed() sniff the content magic bytes and accept such layers;
// the flatten-ctl registry path must too.
func TestPullMislabeledPlainLayer(t *testing.T) {
	host := startRegistry(t)
	blob := plainTarLayer(t)
	d, _, err := v1.SHA256(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, &mislabeledPlainLayer{blob: blob, d: d})
	if err != nil {
		t.Fatalf("build mislabeled image: %v", err)
	}

	ref := host + "/test/mislabeled:v1"
	pushImage(t, ref, img)

	cfg := testConfig(t)
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	cache, err := OpenCache(cfg.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	openers, err := src.Layers()
	if err != nil {
		t.Fatalf("Layers: %v", err)
	}
	if len(openers) != 1 {
		t.Fatalf("got %d layers, want 1", len(openers))
	}
	rc, err := openers[0]()
	if err != nil {
		t.Fatalf("open mislabeled layer: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read mislabeled layer: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatal("mislabeled layer content mismatch")
	}
}
