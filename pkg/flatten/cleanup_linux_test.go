//go:build linux

package flatten

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPreparedReadOnlyTreeCleanup(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires ordinary UID to exercise directory permission cleanup")
	}
	for _, failed := range []bool{false, true} {
		name := "after-output"
		if failed {
			name = "after-layer-failure"
		}
		t.Run(name, func(t *testing.T) {
			scratch := t.TempDir()
			outside := t.TempDir()
			if err := os.WriteFile(filepath.Join(outside, "keep"), []byte("unchanged"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(outside, 0500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(outside, 0700) })
			var owned string
			// Rescue a regressed private tree so the test itself leaves no scratch.
			t.Cleanup(func() {
				if owned != "" {
					os.Chmod(filepath.Join(owned, "rootfs", "readonly"), 0700)
					os.RemoveAll(owned)
				}
			})
			rejected := errors.New("next layer rejected")
			p, err := prepareLayers(context.Background(), []byte("{}"), 2, Options{TmpDir: scratch}, func(i int, root string) error {
				owned = filepath.Dir(root)
				if i == 1 {
					if failed {
						return rejected
					}
					return nil
				}
				entries := []layerEntry{{name: "readonly/file", body: "payload"}}
				for j := 0; j < 260; j++ {
					entries = append(entries, layerEntry{name: fmt.Sprintf("readonly/child-%03d/file", j), body: "payload"})
				}
				entries = append(entries, layerEntry{name: "readonly", kind: tar.TypeDir, mode: 0500}, layerEntry{name: "external", kind: tar.TypeSymlink, link: outside})
				applyTestLayer(t, root, entries...)
				return nil
			}, nil)
			if failed {
				if !errors.Is(err, rejected) {
					t.Fatalf("layer error: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = p.WriteArtifact(context.Background(), io.Discard, "image"); err != nil {
					p.Close()
					t.Fatal(err)
				}
				st, statErr := os.Stat(filepath.Join(owned, "rootfs", "readonly"))
				if statErr != nil || st.Mode().Perm() != 0500 {
					t.Fatalf("source mode changed before cleanup: %v %v", st, statErr)
				}
				if err = p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(scratch)
			if err != nil || len(entries) != 0 {
				t.Fatalf("owned scratch retained: %v %v", entries, err)
			}
			st, err := os.Stat(outside)
			if err != nil || st.Mode().Perm() != 0500 {
				t.Fatalf("external permissions changed: %v %v", st, err)
			}
			b, err := os.ReadFile(filepath.Join(outside, "keep"))
			if err != nil || string(b) != "unchanged" {
				t.Fatal("external target changed")
			}
		})
	}
}

func TestOwnedPermissionRecoveryDescriptorDepth(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires ordinary UID for directory permission recovery")
	}
	const helper = "KUASAR_TEST_OWNED_CLEANUP_DEPTH"
	if os.Getenv(helper) == "" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable, "-test.run=^TestOwnedPermissionRecoveryDescriptorDepth$", "-test.v")
		command.Env = append(os.Environ(), helper+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("cleanup child: %v\n%s", err, output)
		}
		return
	}
	var original unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
		t.Fatal(err)
	}
	const depth = 40
	if original.Cur < depth+32 {
		t.Skip("insufficient inherited descriptors for control calibration")
	}
	withLimit := func(limit uint64, operation func() error) error {
		bounded := original
		bounded.Cur = limit
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &bounded); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
				t.Fatal(err)
			}
		}()
		return operation()
	}
	base := t.TempDir()
	makeTree := func(name string) []string {
		current := name
		directories := []string{name}
		if err := os.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < depth; i++ {
			current = filepath.Join(current, "d")
			if err := os.Mkdir(current, 0700); err != nil {
				t.Fatal(err)
			}
			directories = append(directories, current)
		}
		if err := os.WriteFile(filepath.Join(current, "payload"), []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
		return directories
	}
	// Calibrate against ordinary RemoveAll in this isolated process so runtime
	// bookkeeping FDs cannot make the test depend on a hard-coded spare count.
	var selected uint64
	for limit := uint64(depth + 2); limit <= depth+32; limit++ {
		name := filepath.Join(base, "control")
		makeTree(name)
		err := withLimit(limit, func() error { return os.RemoveAll(name) })
		if cleanErr := os.RemoveAll(name); cleanErr != nil {
			t.Fatal(cleanErr)
		}
		if err == nil {
			selected = limit
			break
		}
		if !errors.Is(err, unix.EMFILE) {
			t.Fatalf("writable control: %v", err)
		}
	}
	if selected == 0 {
		t.Fatal("writable control did not establish a usable limit")
	}
	name := filepath.Join(base, "readonly")
	directories := makeTree(name)
	t.Cleanup(func() {
		for _, directory := range directories {
			os.Chmod(directory, 0700)
		}
		os.RemoveAll(name)
	})
	for i := len(directories) - 1; i > 0; i-- {
		if err := os.Chmod(directories[i], 0); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if err := withLimit(selected, func() error { return removeOwnedWork(name) }); err != nil {
		t.Fatalf("recovery with writable-control limit %d: %v", selected, err)
	}
	if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch retained: %v", err)
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("descriptors before=%d after=%d", len(before), len(after))
	}
}
