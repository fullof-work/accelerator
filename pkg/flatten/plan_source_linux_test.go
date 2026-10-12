//go:build linux

package flatten

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPlanSourceBoundariesAndRead(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := "openat2"
		if fallback {
			name = "os.Root"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			outside := filepath.Join(t.TempDir(), "secret")
			if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
				t.Fatal(err)
			}
			data := bytes.Repeat([]byte("verified source"), 1024)
			if err := os.WriteFile(filepath.Join(directory, "payload"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(directory, "link")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Dir(outside), filepath.Join(directory, "escape")); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			var dir *os.File
			if !fallback {
				dir, err = root.OpenFile(".", unix.O_PATH|unix.O_DIRECTORY, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer dir.Close()
			}
			for _, name := range []string{"link", "escape/secret", "../secret"} {
				f, err := openPlanSource(root, dir, name)
				if err == nil {
					f.Close()
					t.Fatalf("escaped source accepted: %s", name)
				}
			}
			f, err := openPlanSource(root, dir, "payload")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			b := make([]byte, len(data)+1)
			n, err := f.readAt(context.Background(), b, 0)
			if n != len(data) || !errors.Is(err, io.EOF) || !bytes.Equal(b[:n], data) {
				t.Fatalf("short source: %d %v", n, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if n, err = f.readAt(ctx, b, 0); n != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %d %v", n, err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
