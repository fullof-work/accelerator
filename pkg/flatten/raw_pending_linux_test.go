package flatten

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

func TestRawPendingRestrictiveUmask(t *testing.T) {
	const helper = "KUASAR_TEST_Raw_UMASK"
	if mode := os.Getenv(helper); mode != "" {
		if mode == "legacy-kernel" {
			// Exercise the Linux 6.1 path on newer test hosts without changing
			// production behavior or requiring a different kernel.
			runtime.LockOSThread()
			filter := []unix.SockFilter{
				{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
				{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_FCHMODAT2, Jf: 1},
				{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.ENOSYS)},
				{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
			}
			program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
			if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
				t.Fatal(err)
			}
			if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&program)), 0, 0); err != nil {
				t.Fatal(err)
			}
		}
		dir := t.TempDir()
		syscall.Umask(0777)
		for _, existing := range []bool{false, true} {
			output := filepath.Join(dir, "output")
			if existing {
				if err := os.Chmod(output, 0640); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := reserveRawOutput(output)
			if err != nil {
				t.Fatal(err)
			}
			info, err := pending.dir.Stat()
			if err != nil || info.Mode().Perm() != 0700 {
				t.Fatalf("private mode: %v %v", info, err)
			}
			payload := []byte("reserved descriptor remains writable")
			if _, err := pending.file.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := pending.file.ReadAt(got, 0); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("reserved read: %q %v", got, err)
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
			info, err = os.Stat(output)
			want := os.FileMode(0)
			if existing {
				want = 0640
			}
			if err != nil || info.Mode().Perm() != want {
				t.Fatalf("output mode: %v %v, want %o", info, err, want)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 || entries[0].Name() != "output" {
				t.Fatalf("pending cleanup: %v %v", entries, err)
			}
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"current-kernel", "legacy-kernel"} {
		cmd := exec.Command(executable, "-test.run=^TestRawPendingRestrictiveUmask$")
		cmd.Env = append(os.Environ(), helper+"="+mode)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("umask child %s: %v\n%s", mode, err, output)
		}
	}
}

func pendingACL(owner uint16) []byte {
	value := make([]byte, 44)
	binary.LittleEndian.PutUint32(value, 2)
	for i, entry := range [][3]uint32{{1, uint32(owner), 0xffffffff}, {2, 4, 12345}, {4, 0, 0xffffffff}, {16, 4, 0xffffffff}, {32, 0, 0xffffffff}} {
		offset := 4 + i*8
		binary.LittleEndian.PutUint16(value[offset:], uint16(entry[0]))
		binary.LittleEndian.PutUint16(value[offset+2:], uint16(entry[1]))
		binary.LittleEndian.PutUint32(value[offset+4:], entry[2])
	}
	return value
}

func setPendingACL(t *testing.T, path, name string, value []byte) {
	t.Helper()
	if err := unix.Setxattr(path, name, value, 0); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) {
			t.Skip("fixture filesystem has no POSIX ACL support")
		}
		t.Fatal(err)
	}
}

func TestRawPendingAccessACL(t *testing.T) {
	for _, owner := range []uint16{6, 2} {
		t.Run(fmt.Sprintf("owner-%o", owner), func(t *testing.T) {
			dir := t.TempDir()
			output := filepath.Join(dir, "output")
			if err := os.WriteFile(output, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			acl := pendingACL(owner)
			setPendingACL(t, output, "system.posix_acl_access", acl)
			pending, err := reserveRawOutput(output)
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
			got := make([]byte, 64)
			n, err := unix.Lgetxattr(output, "system.posix_acl_access", got)
			if err != nil {
				t.Fatalf("read access ACL: %v", err)
			}
			if !bytes.Equal(got[:n], acl) {
				t.Fatalf("access ACL changed: %x", got[:n])
			}
		})
	}
	t.Run("remove-inherited-acl", func(t *testing.T) {
		dir := t.TempDir()
		output := filepath.Join(dir, "output")
		if err := os.WriteFile(output, []byte("old"), 0640); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(output, 0640); err != nil {
			t.Fatal(err)
		}
		setPendingACL(t, dir, "system.posix_acl_default", pendingACL(7))
		pending, err := reserveRawOutput(output)
		if err != nil {
			t.Fatal(err)
		}
		defer pending.Close()
		if err := pending.closeFile(); err != nil {
			t.Fatal(err)
		}
		if err := pending.commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Lgetxattr(output, "system.posix_acl_access", make([]byte, 64)); !errors.Is(err, unix.ENODATA) {
			t.Fatalf("inherited ACL retained: %v", err)
		}
		info, err := os.Stat(output)
		if err != nil || info.Mode().Perm() != 0640 {
			t.Fatalf("mode: %v %v", info, err)
		}
	})
}

func denyPendingSyscall(t *testing.T, call uintptr, errno unix.Errno) {
	t.Helper()
	runtime.LockOSThread()
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(call), Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(errno)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&program)), 0, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRawPendingFailureBoundaries(t *testing.T) {
	const helper = "KUASAR_TEST_Raw_PENDING_FAILURE"
	if kind := os.Getenv(helper); kind != "" {
		dir := t.TempDir()
		output := filepath.Join(dir, "output")
		switch kind {
		case "access-acl":
			if err := os.WriteFile(output, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			setPendingACL(t, output, "system.posix_acl_access", pendingACL(6))
			denyPendingSyscall(t, unix.SYS_FSETXATTR, unix.EPERM)
			pending, err := reserveRawOutput(output)
			if pending != nil {
				pending.Close()
			}
			if !errors.Is(err, unix.EPERM) {
				t.Fatalf("ACL restoration failure: %v", err)
			}
			got, err := os.ReadFile(output)
			if err != nil || string(got) != "old" {
				t.Fatalf("old output: %q %v", got, err)
			}
			if err := os.Remove(output); err != nil {
				t.Fatal(err)
			}
		case "directory-open":
			var limit unix.Rlimit
			if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
				t.Fatal(err)
			}
			old := limit
			limit.Cur = 128
			if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
				t.Fatal(err)
			}
			fds := []int{}
			for {
				fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
				if errors.Is(err, unix.EMFILE) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				fds = append(fds, fd)
			}
			if len(fds) == 0 {
				t.Fatal("no room for parent descriptor")
			}
			unix.Close(fds[len(fds)-1])
			fds = fds[:len(fds)-1]
			pending, err := reserveRawOutput(output)
			unix.Setrlimit(unix.RLIMIT_NOFILE, &old)
			for _, fd := range fds {
				unix.Close(fd)
			}
			if pending != nil {
				pending.Close()
			}
			if !errors.Is(err, unix.EMFILE) {
				t.Fatalf("directory open failure: %v", err)
			}
		default:
			t.Fatal(kind)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("staging leaked: %v %v", entries, err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"access-acl", "directory-open"} {
		cmd := exec.Command(executable, "-test.run=^TestRawPendingFailureBoundaries$")
		cmd.Env = append(os.Environ(), helper+"="+kind)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", kind, err, out)
		}
	}
}

func TestRawPendingGroupAndEmptyRoot(t *testing.T) {
	const helper = "KUASAR_TEST_Raw_PENDING_GROUP"
	if kind := os.Getenv(helper); kind != "" {
		dir := t.TempDir()
		if err := os.Chown(dir, os.Geteuid(), 12345); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, os.ModeSetgid|0770); err != nil {
			t.Fatal(err)
		}
		if kind == "empty-root" {
			// Only this already-running test child enters its private empty
			// root. There is no procfs, loader or helper executable inside.
			if err := unix.Chroot(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.Chdir("/"); err != nil {
				t.Fatal(err)
			}
			dir = "/"
			denyPendingSyscall(t, unix.SYS_FCHMODAT2, unix.ENOSYS)
		}
		syscall.Umask(0707)
		output := filepath.Join(dir, "output")
		pending, err := reserveRawOutput(output)
		if err != nil {
			t.Fatal(err)
		}
		stage, err := pending.dir.Stat()
		if err != nil || stage.Mode().Perm() != 0700 || stage.Mode()&os.ModeSetgid == 0 {
			t.Fatalf("staging mode: %v %v", stage, err)
		}
		if _, err := pending.file.Write([]byte("image")); err != nil {
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
		info, err := os.Stat(output)
		if err != nil {
			t.Fatal(err)
		}
		if gid := info.Sys().(*syscall.Stat_t).Gid; gid != 12345 {
			t.Fatalf("output GID %d, want 12345", gid)
		}
		if info.Mode().Perm() != 0060 {
			t.Fatalf("umask output mode %o", info.Mode().Perm())
		}
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}
		if kind == "empty-root" {
			os.Exit(0)
		} // Parent owns and removes the host fixture.
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root for distinct GID and chroot fixtures")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"group", "empty-root"} {
		temporary := t.TempDir()
		cmd := exec.Command(executable, "-test.run=^TestRawPendingGroupAndEmptyRoot$")
		cmd.Env = append(os.Environ(), helper+"="+kind, "TMPDIR="+temporary)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", kind, err, out)
		}
	}
}

func TestRawPendingChownOnly(t *testing.T) {
	const helper = "KUASAR_TEST_Raw_CHOWN_ONLY"
	if kind := os.Getenv(helper); kind != "" {
		dir := t.TempDir()
		output := filepath.Join(dir, "output")
		if err := os.WriteFile(output, []byte("old"), 0640); err != nil {
			t.Fatal(err)
		}
		var acl []byte
		if kind == "acl" {
			acl = pendingACL(6)
			setPendingACL(t, output, outputAccessACL, acl)
		}
		if err := os.Chown(output, 12344, 23456); err != nil {
			t.Fatal(err)
		}
		// Capability changes are thread-local. Keep all filesystem calls below on
		// this test child's reduced thread, leaving the parent test runner intact.
		runtime.LockOSThread()
		header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
		caps := [2]unix.CapUserData{{Effective: 1 << unix.CAP_CHOWN, Permitted: 1 << unix.CAP_CHOWN}, {}}
		if err := unix.Capset(&header, &caps[0]); err != nil {
			t.Fatal(err)
		}
		var actual [2]unix.CapUserData
		if err := unix.Capget(&header, &actual[0]); err != nil {
			t.Fatal(err)
		}
		if actual != caps {
			t.Fatalf("capability reduction failed: %+v", actual)
		}
		pending, err := reserveRawOutput(output)
		if err != nil {
			t.Fatal(err)
		}
		defer pending.Close()
		if _, err := pending.file.Write([]byte("new")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 3)
		if _, err := pending.file.ReadAt(got, 0); err != nil || string(got) != "new" {
			t.Fatalf("reserved content: %q %v", got, err)
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
		info, err := os.Stat(output)
		if err != nil {
			t.Fatal(err)
		}
		owner := info.Sys().(*syscall.Stat_t)
		if owner.Uid != 12344 || owner.Gid != 23456 || info.Mode().Perm() != 0640 {
			t.Fatalf("output metadata: %+v", owner)
		}
		gotACL, err := readOutputAccessACL(output)
		if err != nil || !bytes.Equal(gotACL, acl) {
			t.Fatalf("output ACL: %x %v", gotACL, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 1 || entries[0].Name() != "output" {
			t.Fatalf("staging cleanup: %v %v", entries, err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to prepare foreign ownership and reduce capabilities")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"mode", "acl"} {
		cmd := exec.Command(executable, "-test.run=^TestRawPendingChownOnly$")
		cmd.Env = append(os.Environ(), helper+"="+kind)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", kind, err, out)
		}
	}
}

func TestRawPostCommitCleanup(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("ordinary UID required for actual permission failure")
	}
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			dir := t.TempDir()
			output := filepath.Join(dir, "output")
			if err := os.WriteFile(output, []byte("old"), 0644); err != nil {
				t.Fatal(err)
			}
			p, err := reserveRawOutput(output)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.file.Write([]byte("new")); err != nil {
				t.Fatal(err)
			}
			if err = p.closeFile(); err != nil {
				t.Fatal(err)
			}
			if committed {
				if err = p.commit(); err != nil {
					t.Fatal(err)
				}
			}
			if err = os.Chmod(dir, 0555); err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(dir, 0700)

			warned := false
			err = finishRawOutput(p, committed, func(diagnostic error) {
				warned = true
				if !errors.Is(diagnostic, unix.EACCES) {
					t.Errorf("diagnostic: %v", diagnostic)
				}
			})
			if committed {
				if err != nil || !warned {
					t.Fatalf("committed: error=%v warning=%v", err, warned)
				}
			} else if !errors.Is(err, unix.EACCES) || warned {
				t.Fatalf("uncommitted: error=%v warning=%v", err, warned)
			}
			want := "old"
			if committed {
				want = "new"
			}
			data, readErr := os.ReadFile(output)
			if readErr != nil || string(data) != want {
				t.Fatalf("output=%q err=%v want=%s", data, readErr, want)
			}
			// The nonfatal diagnostic still leaves observable, owned cleanup work.
			if _, err := os.Stat(p.path); err != nil {
				t.Fatalf("expected retained empty staging directory: %v", err)
			}
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(p.path); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The directory group deliberately differs from all groups of the child. A
// restrictive umask forces private-directory permission repair by that child.
func TestRawPendingForeignGroup(t *testing.T) {
	const helper = "KUASAR_TEST_Raw_FOREIGN_GROUP"
	if parent := os.Getenv(helper); parent != "" {
		if os.Geteuid() == 0 {
			t.Fatal("fixture must have no root privileges")
		}
		groups, err := os.Getgroups()
		if err != nil {
			t.Fatal(err)
		}
		for _, gid := range groups {
			if gid == 12345 {
				t.Fatal("fixture belongs to destination group")
			}
		}
		for _, mask := range []int{0707, 0777} {
			syscall.Umask(mask)
			direct, err := os.OpenFile(filepath.Join(parent, fmt.Sprintf("direct-%o", mask)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := direct.Stat()
			if err != nil {
				t.Fatal(err)
			}
			direct.Close()
			output := filepath.Join(parent, fmt.Sprintf("output-%o", mask))
			pending, err := reserveRawOutput(output)
			if err != nil {
				t.Fatal(err)
			}
			stage, err := pending.dir.Stat()
			if err != nil || stage.Mode().Perm() != 0700 {
				t.Fatalf("private stage: %v %v", stage, err)
			}
			payload := []byte("inherited group, bound writable descriptor")
			if _, err := pending.file.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := pending.file.ReadAt(got, 0); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("content: %q %v", got, err)
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
			info, err := os.Stat(output)
			if err != nil {
				t.Fatal(err)
			}
			if expected.Sys().(*syscall.Stat_t).Gid != 12345 || info.Sys().(*syscall.Stat_t).Gid != 12345 || info.Mode().Perm() != expected.Mode().Perm() {
				t.Fatalf("output metadata differs from direct creation: got=%+v expected=%+v", info.Sys(), expected.Sys())
			}
		}
		entries, err := os.ReadDir(parent)
		if err != nil || len(entries) != 4 {
			t.Fatalf("reservation leaked: %v %v", entries, err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to create a foreign-group fixture and ordinary child")
	}
	// Use a directly traversable parent rather than the root-only go test build
	// directory. All fixture objects are removed by this parent after the child.
	root, err := os.MkdirTemp("", "flatten-foreign-group-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "destination")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(parent, 0, 12345); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, os.ModeSetgid|0777); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	copy := filepath.Join(root, "fixture.test")
	if err := os.WriteFile(copy, binary, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(copy, "-test.run=^TestRawPendingForeignGroup$")
	cmd.Env = append(os.Environ(), helper+"="+parent)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534, Groups: []uint32{}}}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ordinary child: %v\n%s", err, out)
	}
}
