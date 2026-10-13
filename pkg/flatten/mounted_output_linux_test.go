package flatten

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestMountedRawOutputDestination(t *testing.T) {
	if os.Getenv("FLATTEN_TEST_MOUNTS") != "1" {
		t.Skip("requires a private mount namespace")
	}
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	init, err := os.Readlink("/proc/1/ns/mnt")
	if err != nil || self == init {
		t.Fatal("refusing mount test outside a private namespace", err)
	}
	for _, fileMount := range []bool{true, false} {
		t.Run(map[bool]string{true: "file-mount", false: "mounted-parent"}[fileMount], func(t *testing.T) {
			root := t.TempDir()
			source, target := filepath.Join(root, "source"), filepath.Join(root, "target")
			if fileMount {
				for _, path := range []string{source, target} {
					if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				for _, path := range []string{source, target} {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(source, "image"), []byte("old"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := unix.Unmount(target, 0); err != nil {
					t.Error(err)
				}
			})
			output := target
			if !fileMount {
				output = filepath.Join(target, "image")
			}
			// Both targets permit writing the existing inode. Only the file
			// mount forbids atomic replacement of the directory entry.
			f, err := os.OpenFile(output, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			pending, err := reserveRawOutput(output)
			if fileMount {
				if err == nil {
					commitErr := pending.commit()
					pending.Close()
					t.Fatalf("mounted file was accepted before preparation; commit error: %v", commitErr)
				}
				if !errors.Is(err, unix.EBUSY) {
					t.Fatal(err)
				}
				got, err := os.ReadFile(output)
				if err != nil || string(got) != "old" {
					t.Fatalf("old output: %q %v", got, err)
				}
				entries, err := os.ReadDir(root)
				if err != nil || len(entries) != 2 {
					t.Fatalf("staging leaked: %v %v", entries, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer pending.Close()
				if _, err := pending.file.Write([]byte("new")); err != nil {
					t.Fatal(err)
				}
				if err := pending.closeFile(); err != nil {
					t.Fatal(err)
				}
				if err := pending.commit(); err != nil {
					t.Fatal(err)
				}
				if err := pending.Close(); err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(output)
				if err != nil || string(got) != "new" {
					t.Fatalf("mounted parent output: %q %v", got, err)
				}
			}
		})
	}
}
