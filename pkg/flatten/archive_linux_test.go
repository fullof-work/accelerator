//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func archiveFixture(t *testing.T, codec string) ([]byte, *ImageInput) {
	t.Helper()
	plains := [][]byte{
		testLayerTar(t, layerEntry{name: "dir/old", body: "old"}, layerEntry{name: "keep", body: "kept"}),
		testLayerTar(t, layerEntry{name: "dir/new", body: "new"}, layerEntry{name: "dir/.wh..wh..opq"}),
	}
	var diffIDs []string
	var layers []LayerInput
	files := map[string][]byte{}
	names := []string{"base.tar", "upper.tar"}
	for i, plain := range plains {
		body := plain
		media := "application/vnd.oci.image.layer.v1.tar"
		if codec != "plain" {
			var b bytes.Buffer
			switch codec {
			case "gzip":
				z := gzip.NewWriter(&b)
				z.Write(plain)
				if err := z.Close(); err != nil {
					t.Fatal(err)
				}
			case "zstd":
				z, err := zstd.NewWriter(&b)
				if err != nil {
					t.Fatal(err)
				}
				z.Write(plain)
				if err = z.Close(); err != nil {
					t.Fatal(err)
				}
			}
			body = b.Bytes()
			media += "+" + codec
		}
		diff := fmt.Sprintf("sha256:%x", sha256.Sum256(plain))
		diffIDs = append(diffIDs, diff)
		layers = append(layers, LayerInput{MediaType: media, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), DiffID: diff, Size: int64(len(body)), Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }})
		files[names[i]] = body
	}
	cfg, err := json.Marshal(map[string]any{"architecture": "amd64", "os": "linux", "rootfs": map[string]any{"type": "layers", "diff_ids": diffIDs}, "config": map[string]any{"Cmd": []string{"fixture"}}})
	if err != nil {
		t.Fatal(err)
	}
	files["config.json"] = cfg
	files["manifest.json"], err = json.Marshal([]dockerManifestEntry{{Config: "config.json", Layers: names}})
	if err != nil {
		t.Fatal(err)
	}
	var outer bytes.Buffer
	tw := tar.NewWriter(&outer)
	for _, name := range []string{"upper.tar", "manifest.json", "base.tar", "config.json"} {
		b := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(b))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	in, err := NewImageInput(cfg, layers)
	if err != nil {
		t.Fatal(err)
	}
	return outer.Bytes(), in
}

func TestArchiveFilePipeAndDirectoryEqual(t *testing.T) {
	requirePlan(t)
	for _, codec := range []string{"plain", "gzip", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			outer, in := archiveFixture(t, codec)
			parent := t.TempDir()
			baseline, err := PrepareImage(context.Background(), in, Options{TmpDir: parent})
			if err != nil {
				t.Fatal(err)
			}
			defer baseline.Close()
			var expected bytes.Buffer
			if err = baseline.WriteRaw(context.Background(), &expected); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"file", "pipe", "directory"} {
				t.Run(mode, func(t *testing.T) {
					scratch := t.TempDir()
					var p *PreparedImage
					if mode == "directory" {
						p, err = PrepareDir(context.Background(), baseline.selection.root, Options{TmpDir: baseline.selection.root}, DirOptions{ConfigJSON: in.ConfigJSON()})
					} else {
						var input io.ReadCloser
						if mode == "file" {
							name := filepath.Join(t.TempDir(), "image.tar")
							if e := os.WriteFile(name, outer, 0600); e != nil {
								t.Fatal(e)
							}
							input, err = os.Open(name)
							if err != nil {
								t.Fatal(err)
							}
						} else {
							r, w := io.Pipe()
							input = r
							go func() { _, e := w.Write(outer); w.CloseWithError(e) }()
						}
						p, err = PrepareArchive(context.Background(), input, Options{TmpDir: scratch})
					}
					if err != nil {
						t.Fatal(err)
					}
					defer p.Close()
					spools, _ := filepath.Glob(filepath.Join(scratch, "*.spool"))
					if len(spools) != 0 {
						t.Fatal("spool survived layer application")
					}
					var got bytes.Buffer
					if err = p.WriteRaw(context.Background(), &got); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(expected.Bytes(), got.Bytes()) {
						t.Fatal("equivalent input changed raw output")
					}
					if _, err = os.Stat(filepath.Join(p.selection.root, "dir", "old")); !os.IsNotExist(err) {
						t.Fatal("late opaque failed")
					}
				})
			}
		})
	}
}

func TestArchiveCancellationAndBounds(t *testing.T) {
	for _, kind := range []string{"cancel", "close", "budget", "duplicate", "link", "truncated", "missing-footer", "half-footer", "nonzero-trailer", "concatenated"} {
		t.Run(kind, func(t *testing.T) {
			outer, _ := archiveFixture(t, "plain")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scratch := t.TempDir()
			opts := Options{TmpDir: scratch}
			var input io.ReadCloser = io.NopCloser(bytes.NewReader(outer))
			switch kind {
			case "cancel":
				r, w := io.Pipe()
				defer w.Close()
				input = r
				go func() { time.Sleep(10 * time.Millisecond); cancel() }()
			case "close":
				input = &archiveCloseError{Reader: bytes.NewReader(outer)}
			case "budget":
				opts.ArchiveLimits.Bytes = 1024
			case "truncated":
				input = io.NopCloser(bytes.NewReader(outer[:1000]))
			case "missing-footer":
				input = io.NopCloser(bytes.NewReader(outer[:len(outer)-1024]))
			case "half-footer":
				input = io.NopCloser(bytes.NewReader(outer[:len(outer)-512]))
			case "nonzero-trailer":
				input = io.NopCloser(bytes.NewReader(append(outer, 'x')))
			case "concatenated":
				input = io.NopCloser(bytes.NewReader(append(outer, outer...)))
			case "duplicate", "link":
				var b bytes.Buffer
				tw := tar.NewWriter(&b)
				h := &tar.Header{Name: "member", Mode: 0600}
				if kind == "link" {
					h.Typeflag = tar.TypeSymlink
					h.Linkname = "outside"
				}
				tw.WriteHeader(h)
				if kind == "duplicate" {
					tw.WriteHeader(h)
				}
				tw.Close()
				input = io.NopCloser(&b)
			}
			p, err := PrepareArchive(ctx, input, opts)
			if err == nil {
				p.Close()
				t.Fatal("invalid archive accepted")
			}
			left, err := os.ReadDir(scratch)
			if err != nil || len(left) != 0 {
				t.Fatalf("scratch leak: %v %v", left, err)
			}
		})
	}
}

type archiveCloseError struct{ io.Reader }

func (*archiveCloseError) Close() error { return errors.New("archive close failed") }
