package flatten

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRawPendingRejectedDirectoryCleanup(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to test different filesystem and effective UIDs")
	}
	const helper = "KUASAR_TEST_Raw_REJECTED_DIRECTORY"
	if os.Getenv(helper) == "1" {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0777); err != nil {
			t.Fatal(err)
		}
		previous, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir(directory); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Chdir(previous); err != nil {
				t.Error(err)
			}
		}()
		if err := os.WriteFile("output", []byte("old output"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod("output", 0644); err != nil {
			t.Fatal(err)
		}
		// Limit changed filesystem credentials to this helper's current thread.
		// The kernel creates a differently owned inode without an NFS service.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		old, err := unix.SetfsuidRetUid(65534)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := unix.SetfsuidRetUid(old); err != nil {
				t.Error(err)
			}
		}()
		current, err := unix.SetfsuidRetUid(-1)
		if err != nil || current != 65534 {
			t.Fatalf("filesystem UID: %d %v", current, err)
		}
		pending, err := reserveRawOutput("output")
		if pending != nil {
			pending.Close()
			t.Fatal("unexpectedly accepted differently owned staging")
		}
		if err == nil || !strings.Contains(err.Error(), "staging directory is not private") {
			t.Fatalf("expected private-directory rejection: %v", err)
		}
		entries, err := os.ReadDir(".")
		if err != nil || len(entries) != 1 || entries[0].Name() != "output" {
			t.Fatalf("rejected staging leaked: %v %v", entries, err)
		}
		data, err := os.ReadFile("output")
		if err != nil || string(data) != "old output" {
			t.Fatalf("old output: %q %v", data, err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRawPendingRejectedDirectoryCleanup$", "-test.v")
	cmd.Env = append(os.Environ(), helper+"=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rejected staging cleanup: %v\n%s", err, out)
	}
}
