//go:build linux

package flatten

import (
	"context"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// planSource is a single-consumer, owned descriptor. Regular-file reads do not
// need Go's readiness poller; openat2 resolves the whole path beneath the same
// root in one operation. The os.Root path retains support for older kernels and
// syscall filters. Both paths reject a final symlink and cannot block on a FIFO.
type planSource struct {
	fd       int
	fallback *os.File
}

func openPlanSource(root *os.Root, directory *os.File, name string) (*planSource, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if directory != nil {
		how := unix.OpenHow{Flags: uint64(flags | unix.O_NOATIME), Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS}
		fd, err := unix.Openat2(int(directory.Fd()), name, &how)
		if errors.Is(err, unix.EPERM) {
			how.Flags = uint64(flags)
			fd, err = unix.Openat2(int(directory.Fd()), name, &how)
		}
		if err == nil {
			return &planSource{fd: fd}, nil
		}
	}
	f, err := root.OpenFile(name, flags|unix.O_NOATIME, 0)
	if errors.Is(err, unix.EPERM) {
		f, err = root.OpenFile(name, flags, 0)
	}
	if err != nil {
		return nil, err
	}
	return &planSource{fd: int(f.Fd()), fallback: f}, nil
}

func (f *planSource) readAt(ctx context.Context, b []byte, offset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.fallback != nil {
		return f.fallback.ReadAt(b, offset)
	}
	done := 0
	for done < len(b) {
		if err := ctx.Err(); err != nil {
			return done, err
		}
		n, err := unix.Pread(f.fd, b[done:], offset+int64(done))
		if n > 0 {
			done += n
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return done, err
		}
		if n <= 0 {
			return done, io.EOF
		}
	}
	return done, nil
}

func (f *planSource) Close() error {
	if f.fd < 0 {
		return nil
	}
	fd := f.fd
	f.fd = -1
	if f.fallback != nil {
		return f.fallback.Close()
	}
	return unix.Close(fd)
}
