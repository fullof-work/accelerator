//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

type layerEntry struct {
	name, body, link string
	kind             byte
	mode             int64
	xattrs           map[string]string
}

func testLayerTar(t *testing.T, entries ...layerEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, e := range entries {
		kind, mode := e.kind, e.mode
		if kind == 0 {
			kind = tar.TypeReg
		}
		if mode == 0 {
			mode = 0644
		}
		h := &tar.Header{Name: e.name, Typeflag: kind, Mode: mode, Uid: os.Getuid(), Gid: os.Getgid(), Linkname: e.link, Xattrs: e.xattrs}
		if kind == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := io.WriteString(tw, e.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func applyTestLayer(t *testing.T, root string, entries ...layerEntry) {
	t.Helper()
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{}); err != nil {
		t.Fatal(err)
	}
	if body.closed.Load() != 1 {
		t.Fatal("body close count")
	}
}

func assertLayerFile(t *testing.T, root, name, want string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, name))
	if err != nil || string(got) != want {
		t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
	}
}

func TestUmociLateWhiteouts(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "d/lower", body: "old"},
		layerEntry{name: "replace", body: "old"})
	applyTestLayer(t, root,
		layerEntry{name: "d/upper", body: "new"},
		layerEntry{name: "replace", body: "new"},
		layerEntry{name: "d/.wh..wh..opq"},
		layerEntry{name: ".wh.replace"})
	assertLayerFile(t, root, "d/upper", "new")
	assertLayerFile(t, root, "replace", "new")
	if _, err := os.Lstat(filepath.Join(root, "d/lower")); !os.IsNotExist(err) {
		t.Fatalf("opaque retained lower entry: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, ".wh.replace")); !os.IsNotExist(err) {
		t.Fatal("whiteout leaked")
	}
}

func TestUmociDirectoriesLinksAndReplacement(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "d/lower", body: "keep"}, // implicit parent
		layerEntry{name: "original", body: "old", mode: 0640},
		layerEntry{name: "alias", kind: tar.TypeLink, link: "original", mode: 0640},
		layerEntry{name: "to-dir", body: "old"},
		layerEntry{name: "to-file/child", body: "old"})
	a, _ := os.Stat(filepath.Join(root, "original"))
	b, _ := os.Stat(filepath.Join(root, "alias"))
	if !os.SameFile(a, b) {
		t.Fatal("hardlink identity was lost")
	}
	applyTestLayer(t, root,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 01777},
		layerEntry{name: "original", body: "new", mode: 0600},
		layerEntry{name: "to-dir", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "to-dir/child", body: "new"},
		layerEntry{name: "to-file", body: "new"},
		layerEntry{name: "sym", kind: tar.TypeSymlink, link: "to-dir/child"})
	for name, want := range map[string]string{"d/lower": "keep", "original": "new", "alias": "old", "to-dir/child": "new", "to-file": "new", "sym": "new"} {
		assertLayerFile(t, root, name, want)
	}
	st, err := os.Stat(filepath.Join(root, "d"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode()&(os.ModeSticky|os.ModePerm) != os.ModeSticky|0777 {
		t.Fatalf("directory mode %v", st.Mode())
	}
}

func TestUmociXattrsAndFIFO(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "payload", body: "data", xattrs: map[string]string{"user.kuasar-test": "value"}},
		layerEntry{name: "fifo", kind: tar.TypeFifo, mode: 0600})
	value := make([]byte, 32)
	n, err := unix.Lgetxattr(filepath.Join(root, "payload"), "user.kuasar-test", value)
	if err != nil || string(value[:n]) != "value" {
		t.Fatalf("xattr: %q %v", value[:max(n, 0)], err)
	}
	st, err := os.Lstat(filepath.Join(root, "fifo"))
	if err != nil || st.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO: %v %v", st, err)
	}
	// umoci deliberately omits host-specific SELinux labels. The adapter
	// must return an explicit error rather than silently claiming preservation.
	b := testLayerTar(t, layerEntry{name: "label", body: "x", xattrs: map[string]string{"security.selinux": "not-a-host-policy"}})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err = applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{})
	if err == nil || !strings.Contains(err.Error(), "xattr") {
		t.Fatalf("silent xattr omission: %v", err)
	}
}

func TestUmociEmptyLayerAndMetadataBudget(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root)
	b := testLayerTar(t, layerEntry{name: "parent/child", body: "payload"})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: 1})
	if err == nil || !strings.Contains(err.Error(), "metadata budget") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("budget checked after application")
	}
	if body.closed.Load() != 1 {
		t.Fatal("body leaked on budget error")
	}
}

// Repeated directory headers have bounded retained state, but their old
// cumulative charge exceeded the implicit 128 MiB limit. Builders must not
// require a private library knob to admit this otherwise valid input.
func TestUmociDefaultMetadataBudgetDoesNotRejectRepeatedEntries(t *testing.T) {
	name := strings.Repeat("d/", 64) + "leaf"
	entries := make([]layerEntry, 6000, 6001)
	for i := range entries {
		entries[i] = layerEntry{name: name, kind: tar.TypeDir, mode: 0755}
	}
	entries = append(entries, layerEntry{name: "complete", body: "payload"})
	b := testLayerTar(t, entries...)
	for _, tc := range []struct {
		name      string
		budget    int64
		wantError bool
	}{
		{"default", 0, false},
		{"explicit", 128 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			body := &observedBody{Reader: bytes.NewReader(b)}
			err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: tc.budget})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
					t.Fatalf("explicit limit: %v", err)
				}
				if _, err := os.Stat(filepath.Join(root, "complete")); !os.IsNotExist(err) {
					t.Fatalf("entry beyond explicit budget was applied: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertLayerFile(t, root, "complete", "payload")
			}
			if body.closed.Load() != 1 {
				t.Fatal("body close count")
			}
		})
	}
}

func TestLayerMetadataBudgetNormalization(t *testing.T) {
	for _, n := range []int64{0, 128 << 20, 512 << 20} {
		got, err := (LayerLimits{MetadataBytes: n}).normalized()
		if err != nil || got.MetadataBytes != n {
			t.Fatalf("metadata %d: got %d, %v", n, got.MetadataBytes, err)
		}
		if got.DecodedBytes != 128<<30 || got.DecoderBytes != 64<<20 {
			t.Fatalf("unrelated layer limits changed: %+v", got)
		}
	}
	if _, err := (LayerLimits{MetadataBytes: -1}).normalized(); err == nil {
		t.Fatal("negative explicit metadata limit accepted")
	}
}
