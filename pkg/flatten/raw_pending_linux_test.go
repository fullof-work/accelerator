package flatten

import (
	"bytes"
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
