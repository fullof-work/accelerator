//go:build linux

package flatten

import (
	"context"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

func TestRawOutputPermissions(t *testing.T) {
	requirePlan(t)
	root, outputDir := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	control := filepath.Join(outputDir, "control")
	if err := os.WriteFile(control, nil, 0666); err != nil {
		t.Fatal(err)
	}
	defaultStat, _ := os.Stat(control)
	for _, existing := range []bool{false, true} {
		output := filepath.Join(outputDir, "image")
		want := defaultStat.Mode().Perm()
		if existing {
			if err := os.Chmod(output, 0660); err != nil {
				t.Fatal(err)
			}
			want = 0660
		}
		if err := BuildFromDir(root, output, Options{TmpDir: t.TempDir()}, DirOptions{}); err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(output)
		if err != nil || st.Mode().Perm() != want {
			t.Fatalf("mode got %v err %v want %o", st, err, want)
		}
	}
}

func TestRawOutputRejectsSpecialDestinations(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink("fifo", link); err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{fifo, link, dir} {
		before, err := os.Lstat(output)
		if err != nil {
			t.Fatal(err)
		}
		err = writeRawFile(context.Background(), output, func(string) (*PreparedImage, error) { t.Fatal("special output reached preparation"); return nil, nil })
		if err == nil {
			t.Fatal("accepted special output", output)
		}
		after, e := os.Lstat(output)
		if e != nil || !os.SameFile(before, after) {
			t.Fatal("special output replaced", e)
		}
	}
	if _, _, err := outputPermissions(os.DevNull); err == nil {
		t.Fatal("device accepted")
	}
}
