//go:build linux

package activation

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func listenerFile(t *testing.T, network string) (net.Listener, *os.File) {
	t.Helper()
	address := "127.0.0.1:0"
	if network == "unix" {
		address = filepath.Join(t.TempDir(), "owned.sock")
	}
	if network == "tcp6" {
		address = "[::1]:0"
	}
	l, err := net.Listen(network, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	f, err := l.(interface{ File() (*os.File, error) }).File()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return l, f
}

func fdCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

func TestImportCopiesPreserveOriginalAndManagerListener(t *testing.T) {
	for _, network := range []string{"tcp", "tcp6", "unix"} {
		t.Run(network, func(t *testing.T) {
			manager, original := listenerFile(t, network)
			before := fdCount(t)
			for i := 0; i < 20; i++ {
				copies, err := Import(map[string]*os.File{"data": original}, "fd=data", "127.0.0.1:0", "")
				if err != nil {
					t.Fatal(err)
				}
				copy := copies["data"]
				raw, err := copy.(interface {
					SyscallConn() (syscall.RawConn, error)
				}).SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var flags int
				var socketErr error
				if err := raw.Control(func(fd uintptr) { flags, socketErr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil {
					t.Fatal(err)
				}
				if socketErr != nil || flags&unix.FD_CLOEXEC == 0 {
					t.Fatalf("App copy lacks CLOEXEC: %v", socketErr)
				}
				if err := copy.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if after := fdCount(t); after != before {
				t.Fatalf("listener copies leaked descriptors: %d -> %d", before, after)
			}
			if _, err := original.Stat(); err != nil {
				t.Fatalf("borrowed original closed: %v", err)
			}
			if network == "unix" {
				if st, err := os.Stat(manager.Addr().String()); err != nil || st.Mode()&os.ModeSocket == 0 {
					t.Fatalf("inherited UDS unlinked: %v", err)
				}
			}
			// The manager's listener still actually accepts; connect alone is not
			// enough to prove that closing a copy avoided socket-wide shutdown.
			manager.(interface{ SetDeadline(time.Time) error }).SetDeadline(time.Now().Add(time.Second))
			client, err := net.DialTimeout(network, manager.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			accepted, err := manager.Accept()
			if err != nil {
				t.Fatal(err)
			}
			accepted.Close()
		})
	}
}

func TestImportRejectsInvalidDescriptorsWithoutTakingOwnership(t *testing.T) {
	_, f := listenerFile(t, "tcp")
	regular, err := os.CreateTemp(t.TempDir(), "regular")
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	uf, err := udp.File()
	if err != nil {
		t.Fatal(err)
	}
	defer uf.Close()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	connected := os.NewFile(uintptr(pair[0]), "connected")
	defer connected.Close()
	defer unix.Close(pair[1])
	aliasFD, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	alias := os.NewFile(uintptr(aliasFD), "alias")
	defer alias.Close()
	cases := []struct {
		name      string
		files     map[string]*os.File
		addresses []string
		contains  string
	}{
		{"missing", nil, []string{"fd=data"}, "missing"},
		{"extra", map[string]*os.File{"data": f}, []string{"127.0.0.1:0"}, "unconsumed"},
		{"wrong-name", map[string]*os.File{"other": f}, []string{"fd=data"}, "missing"},
		{"duplicate-entrance", map[string]*os.File{"data": f}, []string{"fd=data", "fd=data"}, "duplicate"},
		{"empty-name", nil, []string{"fd="}, "name"},
		{"invalid-name", nil, []string{"fd=bad:name"}, "name"},
		{"same-file", map[string]*os.File{"a": f, "b": f}, []string{"fd=a", "fd=b"}, "aliases"},
		{"duplicate-socket", map[string]*os.File{"a": f, "b": alias}, []string{"fd=a", "fd=b"}, "aliases"},
		{"regular-file", map[string]*os.File{"data": regular}, []string{"fd=data"}, "not a socket"},
		{"datagram", map[string]*os.File{"data": uf}, []string{"fd=data"}, "not a stream"},
		{"connected-stream", map[string]*os.File{"data": connected}, []string{"fd=data"}, "not a listening"},
	}
	before := fdCount(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				listeners, err := Import(tc.files, tc.addresses...)
				if err == nil || !strings.Contains(err.Error(), tc.contains) || listeners != nil {
					t.Fatalf("Import = %v, %v", listeners, err)
				}
			}
			for name, original := range tc.files {
				if _, err := original.Stat(); err != nil {
					t.Fatalf("closed %s: %v", name, err)
				}
			}
		})
	}
	if after := fdCount(t); after != before {
		t.Fatalf("failed imports leaked FDs: %d -> %d", before, after)
	}
}

// Run in a child so descriptor 3 really is an exec-passed FD and invalid input
// can never overwrite the test runner's runtime/poller descriptors.
func TestActivationEnvChild(t *testing.T) {
	mode := os.Getenv("ACCELERATOR_ACTIVATION_CHILD")
	if mode == "" {
		return
	}
	pid := strconv.Itoa(os.Getpid())
	if mode == "pid" {
		pid = "1"
	}
	os.Setenv("LISTEN_PID", pid)
	files, err := FilesFromEnv()
	if mode == "valid" {
		if err != nil || len(files) != 2 {
			t.Fatalf("FromEnv = %v, %v", files, err)
		}
		for name, f := range files {
			flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
			if err != nil || flags&unix.FD_CLOEXEC == 0 {
				t.Fatalf("original %s lacks CLOEXEC: %v", name, err)
			}
			defer f.Close()
		}
		copies, err := Import(files, "fd=first", "fd=second")
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range copies {
			c.Close()
		}
		for _, key := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
			if _, ok := os.LookupEnv(key); ok {
				t.Fatalf("retained %s", key)
			}
		}
	} else {
		if err == nil || files != nil {
			t.Fatalf("accepted invalid %s environment", mode)
		}
		// Errors do not transfer ownership or close the host's original FDs.
		for _, fd := range []int{3, 4} {
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestFilesFromEnvValidation(t *testing.T) {
	_, first := listenerFile(t, "tcp")
	_, second := listenerFile(t, "unix")
	regular, err := os.CreateTemp(t.TempDir(), "regular")
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()
	for _, tc := range []struct {
		mode, count, names string
		second             *os.File
	}{
		{"valid", "2", "first:second", second},
		{"pid", "2", "first:second", second},
		{"count", "bad", "first:second", second},
		{"negative", "-1", "", second},
		{"excessive", "999999999", "first:second", second},
		{"missing-name", "2", "first", second},
		{"extra-name", "1", "first:second", second},
		{"duplicate-name", "2", "same:same", second},
		{"alias", "2", "first:second", first},
		{"type", "2", "first:second", regular},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestActivationEnvChild$")
			cmd.ExtraFiles = []*os.File{first, tc.second}
			cmd.Env = append(os.Environ(), "ACCELERATOR_ACTIVATION_CHILD="+tc.mode, "LISTEN_FDS="+tc.count, "LISTEN_FDNAMES="+tc.names)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v\n%s", tc.mode, err, out)
			}
		})
	}
}

func TestImportRollbackChild(t *testing.T) {
	if os.Getenv("ACCELERATOR_ACTIVATION_ROLLBACK") != "1" {
		return
	}
	// Initialize the net poller before imposing the descriptor limit. Exactly
	// one descriptor slot remains: one listener copy succeeds, the next fails.
	warm, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	warm.Close()
	first, second := os.NewFile(3, "first"), os.NewFile(4, "second")
	defer first.Close()
	defer second.Close()
	before := fdCount(t)
	var previous unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &previous); err != nil {
		t.Fatal(err)
	}
	limited := previous
	firstFree := 3
	for {
		_, err := unix.FcntlInt(uintptr(firstFree), unix.F_GETFD, 0)
		if errors.Is(err, unix.EBADF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		firstFree++
	}
	limited.Cur = uint64(firstFree + 1)
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limited); err != nil {
		t.Fatal(err)
	}
	defer unix.Setrlimit(unix.RLIMIT_NOFILE, &previous)
	listeners, err := Import(map[string]*os.File{"first": first, "second": second}, "fd=first", "fd=second")
	if !errors.Is(err, unix.EMFILE) || listeners != nil {
		t.Fatalf("partial import = %v, %v", listeners, err)
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &previous); err != nil {
		t.Fatal(err)
	}
	if after := fdCount(t); after != before {
		t.Fatalf("partial import leaked: %d -> %d", before, after)
	}
	for _, file := range []*os.File{first, second} {
		if _, err := file.Stat(); err != nil {
			t.Fatalf("partial import closed original: %v", err)
		}
	}
}

func TestImportRollsBackEarlierCopies(t *testing.T) {
	_, first := listenerFile(t, "tcp")
	_, second := listenerFile(t, "unix")
	cmd := exec.Command(os.Args[0], "-test.run=^TestImportRollbackChild$")
	cmd.ExtraFiles = []*os.File{first, second}
	cmd.Env = append(os.Environ(), "ACCELERATOR_ACTIVATION_ROLLBACK=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rollback: %v\n%s", err, out)
	}
}

func ExampleImport() {
	_, err := Import(nil, "fd=data")
	fmt.Println(err)
	// Output: activation: missing fd=data
}
