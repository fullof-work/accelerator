package flatten

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/image"
)

var unixEpoch = time.Unix(0, 0)

func TestNormalizeSkip(t *testing.T) {
	cases := map[string]string{
		"a/b":    "a/b",
		"/a/b":   "a/b",
		"./a/b/": "a/b",
		"a//b":   "a/b",
	}
	for in, want := range cases {
		got, err := normalizeSkip(in)
		if err != nil || got != want {
			t.Errorf("normalizeSkip(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "/", ".", "..", "../x", "a/../.."} {
		if got, err := normalizeSkip(bad); err == nil {
			t.Errorf("normalizeSkip(%q) = %q, want error", bad, got)
		}
	}
}

func TestParseMountinfo(t *testing.T) {
	fixture := strings.Join([]string{
		`25 30 0:23 / /proc rw,nosuid - proc proc rw`,
		`26 30 0:24 / /rootfs/proc rw - proc proc rw`,
		`27 30 0:25 / /rootfs/dev rw - devtmpfs devtmpfs rw`,
		`28 30 0:26 / /rootfs rw - ext4 /dev/sda1 rw`,
		`29 30 0:27 / /rootfs/mnt/data\040disk rw - ext4 /dev/sdb1 rw`,
		`30 30 0:28 / /rootfsfoo rw - tmpfs tmpfs rw`,
		`bogus line`,
	}, "\n")
	got, err := parseMountinfo(strings.NewReader(fixture), "/rootfs")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"dev", "mnt/data disk", "proc"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("mounts = %v, want %v", got, want)
	}

	// root == "/" — the in-guest full-rootfs export. Every mount except "/"
	// itself must be returned (a naive root+"/" prefix matches nothing here,
	// which once let mkfs walk /proc).
	got, err = parseMountinfo(strings.NewReader(fixture), "/")
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"proc", "rootfs", "rootfs/dev", "rootfs/mnt/data disk", "rootfs/proc", "rootfsfoo"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("mounts under / = %v, want %v", got, want)
	}
}

func TestUnescapeMountPath(t *testing.T) {
	if got := unescapeMountPath(`/a\040b\011c\134d`); got != "/a b\tc\\d" {
		t.Errorf("unescape = %q", got)
	}
	if got := unescapeMountPath(`/plain`); got != "/plain" {
		t.Errorf("plain = %q", got)
	}
}

func requireLinuxExport(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("EROFS generation requires Linux")
	}
}

// fixtureRootfs builds a small tree with a subtree to exclude.
func fixtureRootfs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for p, c := range map[string]string{
		"bin/app":        "#!/bin/sh\necho hi\n",
		"etc/conf":       "k=v\n",
		"var/cache/blob": strings.Repeat("x", 8192),
	} {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(c), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestBuildFromDir(t *testing.T) {
	requireLinuxExport(t)
	root := fixtureRootfs(t)
	out := filepath.Join(t.TempDir(), "img.erofs")

	cfgJSON := []byte(`{"Architecture":"amd64","Os":"linux","Entrypoint":["/bin/app"],"Env":["A=1"]}`) // projected shape
	err := BuildFromDir(root, out, Options{}, DirOptions{
		Skip:       []string{"var/cache"},
		ConfigJSON: cfgJSON,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The appended config round-trips through the projected schema.
	rc, err := image.ReadConfigFromFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Architecture != "amd64" || rc.Os != "linux" ||
		len(rc.Entrypoint) != 1 || rc.Entrypoint[0] != "/bin/app" ||
		len(rc.Env) != 1 || rc.Env[0] != "A=1" {
		t.Errorf("config round-trip mismatch: %+v", rc)
	}

	// The excluded subtree is absent, the rest present (needs fsck.erofs).
	fsck, err := exec.LookPath("fsck.erofs")
	if err != nil {
		t.Log("fsck.erofs not available; skipping content assertions")
		return
	}
	xdir := filepath.Join(t.TempDir(), "x")
	if outB, err := exec.Command(fsck, "--extract="+xdir, out).CombinedOutput(); err != nil {
		t.Fatalf("fsck.erofs: %v: %s", err, outB)
	}
	if _, err := os.Stat(filepath.Join(xdir, "etc/conf")); err != nil {
		t.Errorf("etc/conf missing from image: %v", err)
	}
	if _, err := os.Stat(filepath.Join(xdir, "var/cache")); !os.IsNotExist(err) {
		t.Error("excluded var/cache still present (node should be gone entirely)")
	}
	if _, err := os.Stat(filepath.Join(xdir, "var")); err != nil {
		t.Errorf("sibling var/ lost: %v", err)
	}
}

func TestBuildFromDirDeterministic(t *testing.T) {
	requireLinuxExport(t)
	root := fixtureRootfs(t)
	dir := t.TempDir()
	out1 := filepath.Join(dir, "a.erofs")
	out2 := filepath.Join(dir, "b.erofs")

	if err := BuildFromDir(root, out1, Options{}, DirOptions{}); err != nil {
		t.Fatal(err)
	}
	// Touch mtimes between builds: the source tree must not need
	// normalization (and must not be modified).
	if err := os.Chtimes(filepath.Join(root, "etc/conf"), unixEpoch.AddDate(50, 0, 0), unixEpoch.AddDate(50, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := BuildFromDir(root, out2, Options{}, DirOptions{}); err != nil {
		t.Fatal(err)
	}
	b1, err := os.ReadFile(out1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := os.ReadFile(out2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Error("images differ across mtime changes (determinism broken)")
	}
}

func TestBuildFromDirRootGuard(t *testing.T) {
	err := BuildFromDir("/", filepath.Join(t.TempDir(), "x.erofs"), Options{}, DirOptions{})
	if err == nil || !strings.Contains(err.Error(), "--skip-mounts") {
		t.Errorf("exporting / without --skip-mounts must be refused, got %v", err)
	}
}

func TestBuildFromDirOutputSelfExclusion(t *testing.T) {
	requireLinuxExport(t)
	root := fixtureRootfs(t)
	out := filepath.Join(root, "self.erofs") // output inside the rootfs
	var warned []string
	err := BuildFromDir(root, out, Options{}, DirOptions{
		Warnf: func(f string, a ...any) { warned = append(warned, f) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(warned) == 0 {
		t.Error("expected an auto-exclusion warning")
	}
}
