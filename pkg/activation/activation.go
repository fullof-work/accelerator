// Package activation explicitly imports named listening descriptors for an App.
// Apps never discover process environment on their own.
package activation

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// FilesFromEnv reads systemd's LISTEN_PID, LISTEN_FDS and LISTEN_FDNAMES.
// With no activation environment it returns nil. Otherwise PID, count, names
// and listening sockets must all be valid. Only Linux supports activation.
//
// On success the caller owns the original descriptors (starting at 3), must
// close every returned File, and must not close them while an App imports them.
// They are marked close-on-exec and the three environment variables are unset.
// On error no original descriptor is closed and no File ownership transfers.
// No duplicated descriptor is left behind on failure. The CLI exits on error;
// custom hosts retain responsibility for their original descriptors.
func FilesFromEnv() (map[string]*os.File, error) {
	pid, havePID := os.LookupEnv("LISTEN_PID")
	count, haveCount := os.LookupEnv("LISTEN_FDS")
	names, haveNames := os.LookupEnv("LISTEN_FDNAMES")
	if !havePID && !haveCount && !haveNames {
		return nil, nil
	}
	return filesFromEnv(pid, count, names)
}

// Import validates the complete declared entrance set and duplicates its named
// descriptors as App-owned listeners. Every supplied File must be consumed by
// exactly one fd=<name> address. Ordinary bind addresses are ignored here.
// Missing, extra or aliased sockets are errors; no fallback bind is performed.
//
// The caller retains all input Files and must keep the map and Files stable
// during Import. Each returned listener owns a close-on-exec duplicate. Closing
// it does not unlink an inherited UDS pathname or shut down the shared socket.
// net.FileListener puts the shared socket into Go's nonblocking I/O mode; this
// status flag is shared with original descriptors, unlike FD_CLOEXEC.
// On failure all newly created listeners are closed; inputs remain caller-owned.
func Import(files map[string]*os.File, addresses ...string) (map[string]net.Listener, error) {
	wanted := make(map[string]bool)
	for _, address := range addresses {
		if !strings.HasPrefix(address, "fd=") {
			continue
		}
		name := strings.TrimPrefix(address, "fd=")
		if err := validName(name); err != nil {
			return nil, err
		}
		if wanted[name] {
			return nil, fmt.Errorf("activation: duplicate entrance fd=%s", name)
		}
		wanted[name] = true
		if files[name] == nil {
			return nil, fmt.Errorf("activation: missing fd=%s", name)
		}
	}
	for name := range files {
		if err := validName(name); err != nil {
			return nil, err
		}
		if !wanted[name] {
			return nil, fmt.Errorf("activation: unconsumed fd=%s", name)
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	return importFiles(files)
}

func validName(name string) error {
	if len(name) == 0 || len(name) > 255 {
		return fmt.Errorf("activation: invalid descriptor name %q", name)
	}
	for _, c := range []byte(name) {
		if c < 32 || c > 126 || c == ':' {
			return fmt.Errorf("activation: invalid descriptor name %q", name)
		}
	}
	return nil
}
