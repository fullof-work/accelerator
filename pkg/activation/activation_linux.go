//go:build linux

package activation

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type socketID struct{ device, inode uint64 }

func inspect(fd int) (socketID, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return socketID{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return socketID{}, errors.New("not a socket")
	}
	kind, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return socketID{}, err
	}
	if kind != unix.SOCK_STREAM {
		return socketID{}, errors.New("not a stream socket")
	}
	listening, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
	if err != nil {
		return socketID{}, err
	}
	if listening != 1 {
		return socketID{}, errors.New("not a listening socket")
	}
	address, err := unix.Getsockname(fd)
	if err != nil {
		return socketID{}, err
	}
	switch address.(type) {
	case *unix.SockaddrInet4, *unix.SockaddrInet6, *unix.SockaddrUnix:
	default:
		return socketID{}, errors.New("unsupported socket address family")
	}
	return socketID{uint64(stat.Dev), stat.Ino}, nil
}

func checkSockets(fds map[string]int) error {
	seen := make(map[socketID]string)
	for name, fd := range fds {
		id, err := inspect(fd)
		if err != nil {
			return fmt.Errorf("activation: fd=%s: %w", name, err)
		}
		if previous, ok := seen[id]; ok {
			return fmt.Errorf("activation: fd=%s aliases fd=%s", name, previous)
		}
		seen[id] = name
	}
	return nil
}

func filesFromEnv(pidValue, countValue, namesValue string) (map[string]*os.File, error) {
	pid, err := strconv.Atoi(pidValue)
	if err != nil || pid != os.Getpid() {
		return nil, fmt.Errorf("activation: LISTEN_PID %q does not match this process", pidValue)
	}
	count, err := strconv.Atoi(countValue)
	if err != nil || count < 0 {
		return nil, fmt.Errorf("activation: invalid LISTEN_FDS %q", countValue)
	}
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return nil, err
	}
	if uint64(count) > limit.Cur || uint64(count)+3 > limit.Cur {
		return nil, errors.New("activation: LISTEN_FDS exceeds descriptor limit")
	}
	var names []string
	if namesValue != "" {
		names = strings.Split(namesValue, ":")
	}
	if len(names) != count {
		return nil, errors.New("activation: LISTEN_FDNAMES count differs from LISTEN_FDS")
	}
	fds := make(map[string]int, count)
	for i, name := range names {
		if err := validName(name); err != nil {
			return nil, err
		}
		if _, ok := fds[name]; ok {
			return nil, fmt.Errorf("activation: duplicate descriptor name %q", name)
		}
		fds[name] = 3 + i
	}
	if err := checkSockets(fds); err != nil {
		return nil, err
	}
	// All inputs have been checked before taking ownership. FD_CLOEXEC is per
	// descriptor, not a socket-wide flag on the manager's listening socket.
	for _, fd := range fds {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			return nil, err
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
			return nil, err
		}
	}
	files := make(map[string]*os.File, count)
	for name, fd := range fds {
		files[name] = os.NewFile(uintptr(fd), "activation:"+name)
	}
	for _, name := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		_ = os.Unsetenv(name)
	}
	return files, nil
}

func importFiles(files map[string]*os.File) (listeners map[string]net.Listener, retErr error) {
	fds := make(map[string]int, len(files))
	for name, file := range files {
		fds[name] = int(file.Fd())
	}
	if err := checkSockets(fds); err != nil {
		return nil, err
	}
	listeners = make(map[string]net.Listener, len(files))
	defer func() {
		if retErr != nil {
			for _, listener := range listeners {
				retErr = errors.Join(retErr, listener.Close())
			}
			listeners = nil
		}
	}()
	for name, file := range files {
		listener, err := net.FileListener(file)
		if err != nil {
			return listeners, fmt.Errorf("activation: import fd=%s: %w", name, err)
		}
		if uds, ok := listener.(*net.UnixListener); ok {
			uds.SetUnlinkOnClose(false)
		}
		listeners[name] = listener
	}
	return listeners, nil
}
