//go:build linux

package flatten

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedReadOnlyTreeCleanup(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires ordinary UID to exercise directory permission cleanup")
	}
	requirePlan(t)
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
				applyTestLayer(t, root, layerEntry{name: "readonly/file", body: "payload"}, layerEntry{name: "readonly", kind: tar.TypeDir, mode: 0500}, layerEntry{name: "external", kind: tar.TypeSymlink, link: outside})
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
