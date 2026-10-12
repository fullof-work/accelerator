//go:build linux

package remote

import (
	"errors"
	"os"
	"syscall"
	"time"
)

func pinPendingBlob(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX) }
func pinBlob(f *os.File) error        { return syscall.Flock(int(f.Fd()), syscall.LOCK_SH) }

// Open readers hold a shared inode lease. GC takes an exclusive, nonblocking
// lease and checks the current path identity and age again before unlinking.
// A just-opened Get either pins that inode or notices it was already removed.
func removeIdleBlob(path string, cutoff time.Time) (bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	a, err := f.Stat()
	if err != nil {
		return false, err
	}
	b, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !os.SameFile(a, b) || a.ModTime().After(cutoff) {
		return false, nil
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// lockFile takes an exclusive advisory lock on path (creating it), returning
// an unlock func. Serialises the cache GC sweep across flatten-ctl processes
// so concurrent prunes don't race on the same blobs.
func lockFile(path string) (func(), error) {
	return lockFileMode(path, syscall.LOCK_EX)
}

// Publication spans checked writer Close through link/unlink. A shared GC lock
// covers that otherwise unleased interval; concurrent publishers may proceed.
func lockPublication(path string) (func(), error) {
	return lockFileMode(path, syscall.LOCK_SH)
}

func lockFileMode(path string, mode int) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
