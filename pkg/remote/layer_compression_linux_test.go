//go:build linux

package remote

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/compress/zstd"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
	erofs "github.com/kuasar-sandbox/go-erofs"
)

// The fixture reports the stored and decoded identities truthfully while the
// descriptor deliberately disagrees with the stored compression. Registry
// upload must preserve that mismatch for the production Prepare path to see it.
type labeledCompressionLayer struct {
	blob, plain    []byte
	digest, diffID v1.Hash
	media          types.MediaType
}

func (l labeledCompressionLayer) Compressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.blob)), nil
}

func (l labeledCompressionLayer) Uncompressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.plain)), nil
}

func (l labeledCompressionLayer) Digest() (v1.Hash, error)            { return l.digest, nil }
func (l labeledCompressionLayer) DiffID() (v1.Hash, error)            { return l.diffID, nil }
func (l labeledCompressionLayer) Size() (int64, error)                { return int64(len(l.blob)), nil }
func (l labeledCompressionLayer) MediaType() (types.MediaType, error) { return l.media, nil }

func TestRegistryPrepareMislabeledCompression(t *testing.T) {
	// The tar fixture uses this process's UID/GID, so ordinary CI users exercise
	// the real importer without requiring arbitrary-ownership capabilities.
	for _, tc := range []struct {
		name  string
		codec string
		media types.MediaType
	}{
		{"plain-as-gzip", "plain", types.DockerLayer},
		{"gzip-as-plain", "gzip", types.OCIUncompressedLayer},
		{"zstd-as-gzip", "zstd", types.DockerLayer},
		{"zstd-skippable-as-gzip", "zstd-skippable", types.DockerLayer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plain := plainTarLayer(t)
			blob := plain
			var encoded bytes.Buffer
			switch tc.codec {
			case "gzip":
				w := gzip.NewWriter(&encoded)
				if _, err := w.Write(plain); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				blob = encoded.Bytes()
			case "zstd", "zstd-skippable":
				w, err := zstd.NewWriter(&encoded, zstd.WithEncoderConcurrency(1))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write(plain); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				blob = encoded.Bytes()
				if tc.codec == "zstd-skippable" {
					header := zstd.Header{Skippable: true, SkippableSize: 4}
					prefix, err := header.AppendTo(nil)
					if err != nil {
						t.Fatal(err)
					}
					prefix = append(prefix, "meta"...)
					blob = append(prefix, blob...)
				}
			}
			layer := labeledCompressionLayer{
				blob: blob, plain: plain, media: tc.media,
				digest: hashOf(t, blob), diffID: hashOf(t, plain),
			}
			img, err := mutate.AppendLayers(empty.Image, layer)
			if err != nil {
				t.Fatal(err)
			}
			config, err := img.ConfigFile()
			if err != nil {
				t.Fatal(err)
			}
			config.OS, config.Architecture = "linux", "amd64"
			img, err = mutate.ConfigFile(img, config)
			if err != nil {
				t.Fatal(err)
			}
			configJSON, err := img.RawConfigFile()
			if err != nil {
				t.Fatal(err)
			}

			// Docker-save lacks a layer media type. It must produce the same
			// filesystem and artifact from the exact same stored blob bytes.
			archive := compressionSaveArchive(t, configJSON, blob)
			p, err := flatten.PrepareArchive(context.Background(), io.NopCloser(bytes.NewReader(archive)), flatten.Options{TmpDir: t.TempDir()})
			if err != nil {
				t.Fatalf("PrepareArchive: %v", err)
			}
			want := compressionPreparedImage(t, p)

			var layerReads atomic.Int32
			server := registry.New()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/"+layer.digest.String()) {
					layerReads.Add(1)
				}
				server.ServeHTTP(w, r)
			}))
			defer srv.Close()
			ref := strings.TrimPrefix(srv.URL, "http://") + "/test/mislabeled:latest"
			pushImage(t, ref, img)

			for _, cached := range []bool{false, true} {
				mode := "uncached"
				if cached {
					mode = "cached"
				}
				t.Run(mode, func(t *testing.T) {
					cfg := &Config{Insecure: true, Platform: "linux/amd64"}
					if cached {
						cfg.Cache = CacheConfig{Dir: t.TempDir(), InFlightBytes: "1MiB"}
					}
					if err := cfg.normalize(); err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					res, err := cfg.Resolve(ctx, ref)
					if err != nil {
						t.Fatal(err)
					}
					in, err := cfg.Stream(ctx, res)
					if err != nil {
						t.Fatal(err)
					}
					if got := in.Layers()[0].MediaType; got != string(tc.media) {
						t.Fatalf("registry changed the fixture media type: got %q, want %q", got, tc.media)
					}
					attempts := []string{"cold"}
					if cached {
						attempts = append(attempts, "hit")
					}
					before := layerReads.Load()
					for _, attempt := range attempts {
						if !t.Run(attempt, func(t *testing.T) {
							scratch := t.TempDir()
							p, err := cfg.Prepare(ctx, res, flatten.Options{TmpDir: scratch})
							if err != nil {
								t.Fatalf("Prepare: %v", err)
							}
							if got := compressionPreparedImage(t, p); !bytes.Equal(got, want) {
								t.Fatal("registry and archive produced different images")
							}
							if got := layerReads.Load() - before; got != 1 {
								t.Fatalf("layer downloads: got %d, want exactly one across preparation and cache reuse", got)
							}
							if cached {
								cache, err := OpenCache(cfg.Cache.Dir, cfg.maxSize)
								if err != nil {
									t.Fatal(err)
								}
								if !cache.Has(layer.digest) {
									t.Fatal("verified blob was not committed to the persistent cache")
								}
								pending, err := filepath.Glob(filepath.Join(cfg.Cache.Dir, "blobs", "sha256", ".tmp-*"))
								if err != nil || len(pending) != 0 {
									t.Fatalf("pending cache writes remain: %v %v", pending, err)
								}
							}
							entries, err := os.ReadDir(scratch)
							if err != nil || len(entries) != 0 {
								t.Fatalf("prepared scratch remains: %v %v", entries, err)
							}
						}) {
							break // A failed cold preparation cannot exercise a cache hit.
						}
					}
				})
			}
		})
	}
}

func compressionSaveArchive(t *testing.T, config, layer []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, entry := range []struct {
		name string
		body []byte
	}{
		{"manifest.json", []byte(`[{"Config":"config.json","Layers":["layer.tar"]}]`)},
		{"config.json", config},
		{"layer.tar", layer},
	} {
		if err := w.WriteHeader(&tar.Header{Name: entry.name, Mode: 0644, Size: int64(len(entry.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func compressionPreparedImage(t *testing.T, p *flatten.PreparedImage) []byte {
	t.Helper()
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	var raw bytes.Buffer
	if err := p.WriteRaw(context.Background(), &raw); err != nil {
		t.Fatal(err)
	}
	img, err := erofs.Open(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	got, err := fs.ReadFile(img, "hello.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("exported hello.txt: got %q, error %v", got, err)
	}
	return raw.Bytes()
}
