package util

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// UnixAddr reports whether addr names a Unix domain socket and returns its
// filesystem path. A "unix:" / "unix://" scheme prefix or a leading "/" selects
// unix; anything else (a "host:port") is tcp. The path is returned verbatim
// (callers that need it absolute should hold an absolute addr).
func UnixAddr(addr string) (path string, ok bool) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return strings.TrimPrefix(addr, "unix://"), true
	case strings.HasPrefix(addr, "unix:"):
		return strings.TrimPrefix(addr, "unix:"), true
	case strings.HasPrefix(addr, "/"):
		return addr, true
	default:
		return "", false
	}
}

// Listen binds a stream listener for addr, choosing the network from its form:
// a unix path ("unix:/p", "unix://p", or a leading "/") binds a Unix domain
// socket; anything else binds tcp ("host:port").
//
// For a unix socket Listen creates the parent directory, clears a *stale*
// socket left by a previous run (only when the existing path is itself a socket
// with no live listener — a live socket yields "address already in use", and a
// non-socket path is never removed, so a misconfigured addr can't clobber a
// regular file or directory), binds, and chmods the socket to 0600 (node-local,
// same-user access). The returned *net.UnixListener unlinks the socket on Close.
func Listen(addr string) (net.Listener, error) {
	path, ok := UnixAddr(addr)
	if !ok {
		return net.Listen("tcp", addr)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("listen unix %s: mkdir: %w", path, err)
		}
	}
	if err := clearStaleSocket(path); err != nil {
		return nil, err
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		lis.Close()
		return nil, fmt.Errorf("listen unix %s: chmod: %w", path, err)
	}
	return lis, nil
}

// ListenActivated resolves only explicitly requested, already validated App
// listener copies. Inherited UDS never enter ordinary bind/stale-path cleanup.
func ListenActivated(addr string, inherited map[string]net.Listener) (net.Listener, error) {
	if strings.HasPrefix(addr, "fd=") {
		listener := inherited[strings.TrimPrefix(addr, "fd=")]
		if listener == nil {
			return nil, fmt.Errorf("activation: missing listener %s", addr)
		}
		return listener, nil
	}
	return Listen(addr)
}

// clearStaleSocket removes a leftover unix socket so a fresh bind can succeed,
// but only when it is safe to do so: a path that is not a socket is left in
// place (we must never clobber a regular file or directory), and a socket that
// still has a listener is reported as in-use rather than stolen.
func clearStaleSocket(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("listen unix %s: stat: %w", path, err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("listen unix %s: path exists and is not a socket", path)
	}
	// A live daemon may still own it; a successful connect means the bind is
	// taken — don't unlink it out from under the running process.
	if c, derr := net.DialTimeout("unix", path, 200*time.Millisecond); derr == nil {
		c.Close()
		return fmt.Errorf("listen unix %s: address already in use", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("listen unix %s: remove stale socket: %w", path, err)
	}
	return nil
}
