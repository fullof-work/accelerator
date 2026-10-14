//go:build linux

package remote

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
	"github.com/kuasar-sandbox/accelerator/pkg/image"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func streamingImage(t *testing.T, marker string) v1.Image {
	t.Helper()
	var body bytes.Buffer
	tw := tar.NewWriter(&body)
	for _, name := range []string{"payload", "duplicate"} {
		data := strings.Repeat(marker, 4096)
		if err := tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(data)), Mode: 0644, Uid: os.Getuid(), Gid: os.Getgid()}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body.Bytes())), nil })
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, l)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Architecture, cfg.OS = "amd64", "linux"
	cfg.Config.Cmd = []string{marker}
	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func TestRegistryStreamFixedIdentityToPlanPipe(t *testing.T) {
	img := streamingImage(t, "original")
	mfst, _ := img.Manifest()
	layerPath := "/v2/test/stream/blobs/" + mfst.Layers[0].Digest.String()
	var reads atomic.Int32
	var broken atomic.Bool
	server := registry.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == layerPath {
			reads.Add(1)
			if broken.Load() {
				rr := httptest.NewRecorder()
				server.ServeHTTP(rr, r)
				for k, vs := range rr.Header() {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(rr.Code)
				b := rr.Body.Bytes()
				w.Write(b[:len(b)/2])
				return
			}
		}
		server.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ref := strings.TrimPrefix(srv.URL, "http://") + "/test/stream:latest"
	pushImage(t, ref, img)
	c := testConfig(t)
	c.Platform = "linux/amd64"
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	resolved, err := c.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	pushImage(t, ref, streamingImage(t, "replacement"))
	input, err := c.Stream(ctx, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 0 {
		t.Fatal("metadata admission downloaded a layer")
	}
	scratch := t.TempDir()
	broken.Store(true)
	if p, err := flatten.PrepareImage(ctx, input, flatten.Options{TmpDir: scratch}); err == nil {
		p.Close()
		t.Fatal("partial body accepted")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed rootfs remains: %v %v", entries, err)
	}
	broken.Store(false)
	p, err := flatten.PrepareImage(ctx, input, flatten.Options{TmpDir: scratch})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, e := p.WriteArtifact(ctx, w, "image")
		ce := w.Close()
		if e == nil {
			e = ce
		}
		done <- e
	}()
	source, _, err := tarstream.SourceFrom(r, "image")
	if err != nil {
		r.Close()
		<-done
		t.Fatal(err)
	}
	data := make([]byte, source.Size())
	for off := uint64(0); off < source.Size(); {
		n, e := source.ReadAt(ctx, data[off:min(off+65536, source.Size())], off)
		if e != nil || n == 0 {
			r.Close()
			<-done
			t.Fatalf("consumer rejected: %v", e)
		}
		off += uint64(n)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	r.Close()
	config, err := image.ReadConfig(bytes.NewReader(data), int64(len(data)))
	if err != nil || fmt.Sprint(config.Cmd) != "[original]" {
		t.Fatalf("tag moved during import: %v %v", config, err)
	}
	entries, err = os.ReadDir(c.CacheDir())
	if err != nil || len(entries) != 0 {
		t.Fatalf("default streaming path populated cache: %v %v", entries, err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatal("prepared scratch remains")
	}
	// The reopenable input survives a failed or closed prepared rootfs.
	p, err = flatten.PrepareImage(ctx, input, flatten.Options{TmpDir: scratch})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 3 {
		t.Fatalf("unexpected layer rereads: %d", reads.Load())
	}
	if leftovers, _ := filepath.Glob(filepath.Join(scratch, "*")); len(leftovers) != 0 {
		t.Fatal(leftovers)
	}
}

func TestRegistryPreparePersistentCacheValidation(t *testing.T) {
	for _, tc := range []struct {
		name, budget          string
		required, wrongDiffID bool
	}{
		{"cache", "1MiB", false, false},
		{"bypass", "1B", false, false},
		{"required", "1B", true, false},
		{"wrong-diffid", "1MiB", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			img := streamingImage(t, "lower")
			upper, err := streamingImage(t, "upper").Layers()
			if err != nil {
				t.Fatal(err)
			}
			img, err = mutate.AppendLayers(img, upper...)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wrongDiffID {
				layers, err := img.Layers()
				if err != nil {
					t.Fatal(err)
				}
				cfg, err := img.ConfigFile()
				if err != nil {
					t.Fatal(err)
				}
				cfg.RootFS.DiffIDs[0] = v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0", 64)}
				img, err = mutate.ConfigFile(img, cfg)
				if err != nil {
					t.Fatal(err)
				}
				img = wrongDiffIDImage{Image: img, layers: layers}
			}
			mfst, err := img.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			var layerReads atomic.Int32
			server := registry.New()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for _, l := range mfst.Layers {
					if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/blobs/"+l.Digest.String()) {
						layerReads.Add(1)
					}
				}
				server.ServeHTTP(w, r)
			}))
			defer srv.Close()
			ref := strings.TrimPrefix(srv.URL, "http://") + "/test/cache:latest"
			pushImage(t, ref, img)
			cfg := &Config{Insecure: true, Platform: "linux/amd64", Cache: CacheConfig{Dir: t.TempDir(), InFlightBytes: tc.budget, Required: tc.required}}
			if err := cfg.normalize(); err != nil {
				t.Fatal(err)
			}
			res, err := cfg.Resolve(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			scratch := t.TempDir()
			p, err := cfg.Prepare(context.Background(), res, flatten.Options{TmpDir: scratch})
			wantFail := tc.required || tc.wrongDiffID
			if (err != nil) != wantFail {
				if p != nil {
					p.Close()
				}
				t.Fatalf("prepare: %v", err)
			}
			if p != nil {
				if _, _, err := p.WriteArtifact(context.Background(), io.Discard, "image"); err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cache, err := OpenCache(cfg.Cache.Dir, cfg.maxSize)
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range mfst.Layers {
				if cache.Has(l.Digest) != (tc.name == "cache") {
					t.Fatalf("unexpected committed layer %s", l.Digest)
				}
			}
			if tc.required && layerReads.Load() != 0 {
				t.Fatal("rejected required-cache layer was downloaded")
			}
			if tc.name == "cache" {
				before := layerReads.Load()
				p, err := cfg.Prepare(context.Background(), res, flatten.Options{TmpDir: scratch})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				if before != 2 || layerReads.Load() != before {
					t.Fatal("cache hit re-downloaded a layer")
				}
				// A damaged persistent hit must fail re-verification before output.
				if err := os.WriteFile(cache.blobPath(mfst.Layers[0].Digest), []byte("damaged"), 0600); err != nil {
					t.Fatal(err)
				}
				p, err = cfg.Prepare(context.Background(), res, flatten.Options{TmpDir: scratch})
				if err == nil {
					p.Close()
					t.Fatal("accepted damaged cache hit")
				}
			}
			entries, err := os.ReadDir(scratch)
			if err != nil || len(entries) != 0 {
				t.Fatalf("private rootfs not cleaned: %v %v", entries, err)
			}
			pending, _ := filepath.Glob(filepath.Join(cache.dir, "blobs", "sha256", ".tmp-*"))
			if len(pending) != 0 {
				t.Fatal("pending writes not joined and removed:", pending)
			}
		})
	}
}

// Preserve the original compressed blob while deliberately publishing a wrong
// config DiffID. ggcr's convenience mutator otherwise refuses this fixture at
// upload time, before the importer being tested can observe it.
type wrongDiffIDImage struct {
	v1.Image
	layers []v1.Layer
}

func (i wrongDiffIDImage) Layers() ([]v1.Layer, error) { return i.layers, nil }

func (i wrongDiffIDImage) LayerByDiffID(h v1.Hash) (v1.Layer, error) {
	if h.Hex == strings.Repeat("0", 64) {
		return i.layers[0], nil
	}
	return i.Image.LayerByDiffID(h)
}

func TestStreamOptionalConfigVariant(t *testing.T) {
	for _, variant := range []string{"", "v7", "v6"} {
		t.Run("config-"+variant, func(t *testing.T) {
			img := streamingImage(t, "arm")
			config, err := img.ConfigFile()
			if err != nil {
				t.Fatal(err)
			}
			config.Architecture, config.Variant = "arm", variant
			img, err = mutate.ConfigFile(img, config)
			if err != nil {
				t.Fatal(err)
			}
			index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
				Add: img, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm", Variant: "v7"}},
			})
			srv := httptest.NewServer(registry.New())
			defer srv.Close()
			ref, err := name.NewTag(strings.TrimPrefix(srv.URL, "http://")+"/variant/image:tag", name.Insecure)
			if err != nil {
				t.Fatal(err)
			}
			if err = ggcrremote.WriteIndex(ref, index); err != nil {
				t.Fatal(err)
			}
			c := testConfig(t)
			if err = c.SetPlatform("linux/arm/v7"); err != nil {
				t.Fatal(err)
			}
			resolved, err := c.Resolve(context.Background(), ref.String())
			if err != nil {
				t.Fatal(err)
			}
			want, _ := img.Digest()
			if resolved.Hash != want {
				t.Fatalf("did not pin selected manifest: %v", resolved.Hash)
			}
			_, err = c.Stream(context.Background(), resolved)
			if (err != nil) != (variant == "v6") {
				t.Fatalf("variant %q: %v", variant, err)
			}
		})
	}
}
