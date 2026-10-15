//go:build linux

package flatten

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// These filesystem fault cases are opt-in and require a private mount namespace.
// They never mount over or fill an existing filesystem or another task's path.
func requirePrivateMounts(t *testing.T) {
	t.Helper()
	if os.Getenv("FLATTEN_TEST_MOUNTS") != "1" {
		t.Skip("requires FLATTEN_TEST_MOUNTS=1 in an isolated mount namespace")
	}
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	init, err := os.Readlink("/proc/1/ns/mnt")
	if err != nil || self == init {
		t.Fatal("refusing mount tests without a private mount namespace:", err)
	}
}

func privateTestMount(t *testing.T, source, target, kind string, flags uintptr, data string) {
	t.Helper()
	if err := unix.Mount(source, target, kind, flags, data); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(target, 0); err != nil {
			t.Error("owned mount teardown:", err)
		}
	})
}

func assertNoOwnedFDs(t *testing.T, roots ...string) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil { // The enumeration descriptor may already be closed.
			continue
		}
		name = strings.TrimSuffix(name, " (deleted)")
		for _, root := range roots {
			if name == root || strings.HasPrefix(name, root+"/") {
				t.Errorf("owned descriptor %s remains open: %s", e.Name(), name)
			}
		}
	}
}

func TestMountedFlattenENOSPC(t *testing.T) {
	requirePrivateMounts(t)
	t.Run("layer-application", func(t *testing.T) {
		limited := t.TempDir()
		privateTestMount(t, "tmpfs", limited, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "size=2m,mode=0700")
		data := testLayerTar(t, layerEntry{name: "payload", body: strings.Repeat("x", 4<<20)})
		body := &observedBody{Reader: bytes.NewReader(data)}
		l := streamInput(data, data, "application/vnd.oci.image.layer.v1.tar", body)
		in, err := NewImageInput([]byte(fmt.Sprintf(`{"rootfs":{"type":"layers","diff_ids":[%q]}}`, l.DiffID)), []LayerInput{l})
		if err != nil {
			t.Fatal(err)
		}
		p, err := PrepareImage(context.Background(), in, Options{TmpDir: limited})
		if p != nil || !errors.Is(err, unix.ENOSPC) {
			if p != nil {
				p.Close()
			}
			t.Fatalf("actual layer filesystem exhaustion: %v", err)
		}
		if body.closed.Load() != 1 {
			t.Fatal("layer body was not closed exactly once")
		}
		entries, err := os.ReadDir(limited)
		if err != nil || len(entries) != 0 {
			t.Fatalf("half-applied rootfs survived: %v %v", entries, err)
		}
		assertNoOwnedFDs(t, limited)
	})
	t.Run("raw-output", func(t *testing.T) {
		limited, root, scratch := t.TempDir(), t.TempDir(), t.TempDir()
		privateTestMount(t, "tmpfs", limited, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "size=1m,mode=0700")
		// Each 4 KiB chunk has a different digest; deduplication cannot hide
		// destination exhaustion behind a repeated-byte test payload.
		var data bytes.Buffer
		for i := 0; i < 1024; i++ {
			h := sha256.Sum256([]byte(fmt.Sprint(i)))
			data.Write(bytes.Repeat(h[:], 128))
		}
		if err := os.WriteFile(filepath.Join(root, "payload"), data.Bytes(), 0640); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(limited, "existing.img")
		if err := os.WriteFile(output, []byte("previous artifact"), 0640); err != nil {
			t.Fatal(err)
		}
		err := BuildFromDir(root, output, Options{TmpDir: scratch}, DirOptions{})
		if !errors.Is(err, unix.ENOSPC) {
			t.Fatalf("actual raw output exhaustion: %v", err)
		}
		assertLayerFile(t, limited, "existing.img", "previous artifact")
		entries, err := os.ReadDir(limited)
		if err != nil || len(entries) != 1 {
			t.Fatalf("pending raw output survived: %v %v", entries, err)
		}
		entries, err = os.ReadDir(scratch)
		if err != nil || len(entries) != 0 {
			t.Fatalf("plan scratch survived: %v %v", entries, err)
		}
		assertNoOwnedFDs(t, root, limited, scratch)
	})
}

func TestMountedSourceSelection(t *testing.T) {
	requirePrivateMounts(t)
	root, alias, external := t.TempDir(), t.TempDir(), t.TempDir()
	for _, p := range []string{"exports", "mount"} {
		if err := os.Mkdir(filepath.Join(root, p), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("stable selected contents"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(external, "excluded"), []byte("mounted contents"), 0600); err != nil {
		t.Fatal(err)
	}
	privateTestMount(t, filepath.Join(root, "exports"), alias, "", unix.MS_BIND, "")
	privateTestMount(t, external, filepath.Join(root, "mount"), "", unix.MS_BIND, "")
	output := filepath.Join(alias, "image.raw")
	if err := os.WriteFile(output, []byte("previous output must be excluded"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "exports", "image.raw"), filepath.Join(root, "output-alias")); err != nil {
		t.Fatal(err)
	}
	if err := BuildFromDir(root, output, Options{TmpDir: alias}, DirOptions{SkipMounts: true}); err != nil {
		t.Fatal(err)
	}
	extracted := filepath.Join(t.TempDir(), "root")
	fsck, err := exec.LookPath("fsck.erofs")
	if err != nil {
		t.Skip("fsck.erofs not available for independent content verification")
	}
	if out, err := exec.Command(fsck, "--extract="+extracted, output).CombinedOutput(); err != nil {
		t.Fatalf("extract actual image: %s %v", out, err)
	}
	assertLayerFile(t, extracted, "payload", "stable selected contents")
	for _, p := range []string{"mount", "output-alias", "exports/image.raw"} {
		if _, err := os.Lstat(filepath.Join(extracted, p)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("excluded path %s entered image: %v", p, err)
		}
	}
	entries, err := os.ReadDir(alias)
	if err != nil || len(entries) != 1 || entries[0].Name() != "image.raw" {
		t.Fatalf("source-contained scratch survived: %v %v", entries, err)
	}
	assertNoOwnedFDs(t, root, alias)
}

func TestMountedGoPlanSemantics(t *testing.T) {
	requirePrivateMounts(t)
	root, work, mounted := t.TempDir(), t.TempDir(), t.TempDir()
	data := bytes.Repeat([]byte("kernel checks compact primary-device chunk references\n"), 400)
	if err := os.WriteFile(filepath.Join(root, "data"), data, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(filepath.Join(root, "data"), 12345, 23456); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "fifo"), filepath.Join(root, "fifo-alias")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mknod(filepath.Join(root, "device"), unix.S_IFCHR|0600, int(unix.Mkdev(1, 3))); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("data", filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(work, "image.raw")
	if err := BuildFromDir(root, output, Options{TmpDir: work}, DirOptions{}); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mount", "-t", "erofs", "-o", "loop,ro", output, mounted).CombinedOutput(); err != nil {
		t.Fatalf("kernel EROFS mount: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := exec.Command("umount", mounted).CombinedOutput(); err != nil {
			t.Errorf("owned EROFS teardown: %v %s", err, out)
		}
	})
	for _, name := range []string{"data", "alias", "symlink"} {
		got, err := os.ReadFile(filepath.Join(mounted, name))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("kernel content %s: %v", name, err)
		}
	}
	for _, pair := range [][2]string{{"data", "alias"}, {"fifo", "fifo-alias"}} {
		a, err := os.Lstat(filepath.Join(mounted, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.Lstat(filepath.Join(mounted, pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(a, b) || a.Sys().(*syscall.Stat_t).Nlink != 2 {
			t.Fatalf("kernel link identity: %v", pair)
		}
	}
	info, err := os.Stat(filepath.Join(mounted, "data"))
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	if st.Uid != 12345 || st.Gid != 23456 || info.Mode().Perm() != 0640 {
		t.Fatalf("kernel ownership/mode: %+v", st)
	}
	info, err = os.Lstat(filepath.Join(mounted, "device"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Sys().(*syscall.Stat_t).Rdev != unix.Mkdev(1, 3) {
		t.Fatal("kernel device number")
	}
}
