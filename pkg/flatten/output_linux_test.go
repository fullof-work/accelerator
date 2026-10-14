//go:build linux

package flatten

import (
	"bytes"
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRawOutputPermissions(t *testing.T) {
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
		err = writeRawFile(context.Background(), output, nil, func(string) (*PreparedImage, error) { t.Fatal("special output reached preparation"); return nil, nil })
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

func TestRawOutputPreservesExistingOwnership(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to replace a foreign-owned output")
	}
	root, dir, scratch := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("raw owner regression"), 0644); err != nil {
		t.Fatal(err)
	}
	control, output := filepath.Join(dir, "control"), filepath.Join(dir, "output")
	if err := BuildFromDir(root, control, Options{TmpDir: scratch}, DirOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("previous output"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(output, 12345, 23456); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(output, 0640); err != nil {
		t.Fatal(err)
	}
	if err := BuildFromDir(root, output, Options{TmpDir: scratch}, DirOptions{}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	owner := info.Sys().(*syscall.Stat_t)
	if owner.Uid != 12345 || owner.Gid != 23456 || info.Mode().Perm() != 0640 {
		t.Fatalf("uid/gid/mode: %d/%d/%o", owner.Uid, owner.Gid, info.Mode().Perm())
	}
	want, err := os.ReadFile(control)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("raw contents differ: %v", err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch remains: %v %v", entries, err)
	}
	entries, err = os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("pending output remains: %v %v", entries, err)
	}
}

func TestRawOutputOwnershipFailure(t *testing.T) {
	if output := os.Getenv("FLATTEN_TEST_FOREIGN_OUTPUT"); output != "" {
		if os.Geteuid() != 65534 {
			t.Fatal("helper credentials not applied")
		}
		err := writeRawFile(context.Background(), output, nil, func(string) (*PreparedImage, error) {
			t.Fatal("ownership failure reached preparation")
			return nil, nil
		})
		if err == nil {
			t.Fatal("ownership change unexpectedly succeeded")
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to set up a foreign-owned output")
	}
	root, err := os.MkdirTemp("", "flatten-owner-failure-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0777); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "output")
	before := []byte("preserve previous artifact")
	if err := os.WriteFile(output, before, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(output, 12345, 23456); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(output, 0666); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The original test binary may be under a private checkout directory.
	// Copy it into this fixture so the unprivileged helper can execute it.
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, "helper")
	if err := os.WriteFile(helper, data, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(helper)
	cmd := exec.Command(helper, "-test.run=^TestRawOutputOwnershipFailure$", "-test.v")
	cmd.Env = append(os.Environ(), "FLATTEN_TEST_FOREIGN_OUTPUT="+output)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	if log, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v\n%s", err, log)
	}
	got, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(got, before) {
		t.Fatalf("previous artifact changed: %v", err)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if stat.Uid != 12345 || stat.Gid != 23456 || info.Mode().Perm() != 0666 {
		t.Fatal("previous metadata changed")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatalf("pending file remains: %v %v", entries, err)
	}
}

func TestRawOutputRejectsReplacedTemporary(t *testing.T) {
	for _, kind := range []string{"symlink", "regular", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root, dir, scratch := t.TempDir(), t.TempDir(), t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "payload"), []byte("raw payload"), 0644); err != nil {
				t.Fatal(err)
			}
			output, sentinel := filepath.Join(dir, "output"), filepath.Join(dir, "sentinel")
			previous, target := []byte("previous raw output"), []byte("untouched target")
			if err := os.WriteFile(output, previous, 0640); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(sentinel, target, 0644); err != nil {
				t.Fatal(err)
			}
			var moved, replacement string
			err := writeRawFile(context.Background(), output, nil, func(pending string) (*PreparedImage, error) {
				if kind == "directory" {
					moved = pending + "-moved"
					if err := os.Rename(pending, moved); err != nil {
						return nil, err
					}
					if err := os.Mkdir(pending, 0700); err != nil {
						return nil, err
					}
					replacement = filepath.Join(pending, "image")
					if err := os.WriteFile(replacement, target, 0644); err != nil {
						return nil, err
					}
				} else {
					file := filepath.Join(pending, "image")
					if err := os.Remove(file); err != nil {
						return nil, err
					}
					if kind == "symlink" {
						if err := os.Symlink(sentinel, file); err != nil {
							return nil, err
						}
					} else if err := os.WriteFile(file, target, 0644); err != nil {
						return nil, err
					}
				}
				return PrepareDir(context.Background(), root, Options{TmpDir: scratch}, DirOptions{OutputPaths: []string{output, pending}})
			})
			if err == nil || !strings.Contains(err.Error(), "temporary object was replaced") {
				t.Fatalf("replacement accepted: %v", err)
			}
			for path, want := range map[string][]byte{output: previous, sentinel: target} {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("changed %s: %v", path, err)
				}
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("replaced %s: %v", path, err)
				}
			}
			entries, err := os.ReadDir(scratch)
			if err != nil || len(entries) != 0 {
				t.Fatalf("scratch remains: %v %v", entries, err)
			}
			if moved != "" {
				entries, err := os.ReadDir(moved)
				if err != nil || len(entries) != 0 {
					t.Fatalf("owned raw remains: %v %v", entries, err)
				}
				got, err := os.ReadFile(replacement)
				if err != nil || !bytes.Equal(got, target) {
					t.Fatalf("replacement tree changed: %v", err)
				}
			} else {
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 2 {
					t.Fatalf("pending output remains: %v %v", entries, err)
				}
			}
		})
	}
}

func TestSourceSelectionOutputFiles(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input"), []byte("input"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(outside, "stream"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, name := range []string{"stdout", "alias"} {
		if err := os.Link(file.Name(), filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	fd, err := unix.Dup(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	borrowed := os.NewFile(uintptr(fd), "no-usable-output-path")
	defer borrowed.Close()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	selection, err := selectSourceWithOwned(context.Background(), root, nil, nil, []*os.File{borrowed, write}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !selection.excluded("stdout") || !selection.excluded("alias") || selection.excluded("input") {
		t.Fatalf("selection: %v", selection.excludes)
	}
	if _, err := borrowed.Write([]byte("output grows")); err != nil {
		t.Fatal(err)
	}
	if err := selection.validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "input"), []byte("changed input"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := selection.validate(context.Background()); err == nil {
		t.Fatal("included source mutation was not rejected")
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := selectSourceWithOwned(context.Background(), root, nil, nil, []*os.File{borrowed}, 1<<20); err == nil {
		t.Fatal("closed output FD accepted")
	}
	if _, err := selectSourceWithOwned(context.Background(), root, nil, nil, []*os.File{nil}, 1<<20); err == nil {
		t.Fatal("nil output FD accepted")
	}
}

func TestSourceSelectionBorrowedPipe(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	flags := func() int {
		t.Helper()
		raw, err := write.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var value int
		var statErr error
		if err := raw.Control(func(fd uintptr) { value, statErr = unix.FcntlInt(fd, unix.F_GETFL, 0) }); err != nil {
			t.Fatal(err)
		}
		if statErr != nil {
			t.Fatal(statErr)
		}
		return value
	}
	before := flags()
	if before&unix.O_NONBLOCK == 0 {
		t.Fatal("fixture pipe is not pollable")
	}
	if _, err := selectSourceWithOwned(context.Background(), t.TempDir(), nil, nil, []*os.File{write}, 1<<20); err != nil {
		t.Fatal(err)
	}
	if after := flags(); after != before {
		t.Fatalf("borrow changed pipe flags: %x -> %x", before, after)
	}
	if err := write.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := write.Write(make([]byte, 1<<20)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("borrowed write deadline: %v", err)
	}
}
