package flatten

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// A private directory protects the name used by renameat from other users of a
// writable destination directory. All I/O stays on the originally reserved FD;
// retained directory FDs also prevent substituted ancestor paths from selecting
// another object at commit or cleanup. No procfs or extra raw output copy is used.
type pendingRawOutput struct {
	file, dir, parent  *os.File
	fileInfo, dirInfo  os.FileInfo
	path, name, output string
	ownedFile          bool
}

func reserveRawOutput(output string) (_ *pendingRawOutput, retErr error) {
	mode, previous, err := outputPermissions(output)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(output)
	fd, err := unix.Open(parent, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	p := &pendingRawOutput{parent: os.NewFile(uintptr(fd), parent)}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, p.Close())
		}
	}()
	for range 10 {
		name := ".flatten-output-" + rand.Text()
		err = unix.Mkdirat(fd, name, 0700)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return nil, err
		}
		p.name, p.path = name, filepath.Join(parent, name)
		break
	}
	if p.name == "" {
		return nil, fmt.Errorf("reserve raw output: %w", os.ErrExist)
	}
	dirFD, err := unix.Openat(fd, p.name, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	p.dir = os.NewFile(uintptr(dirFD), p.path)
	var st unix.Stat_t
	if err := unix.Fstat(dirFD, &st); err != nil {
		return nil, err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
		return nil, errors.New("raw output staging directory is not private")
	}
	p.dirInfo, err = p.dir.Stat()
	if err != nil {
		return nil, err
	}
	fileFD, err := unix.Openat(dirFD, "image", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(mode))
	if err != nil {
		return nil, err
	}
	p.ownedFile = true
	p.file = os.NewFile(uintptr(fileFD), filepath.Join(p.path, "image"))
	if previous != nil {
		uid, gid := int(previous.Uid), int(previous.Gid)
		if err := unix.Fstat(fileFD, &st); err != nil {
			return nil, err
		}
		if st.Uid != uint32(uid) || st.Gid != uint32(gid) {
			if err := p.file.Chown(uid, gid); err != nil {
				return nil, err
			}
		}
		// chown may clear mode bits, so restore permissions afterwards.
		if err := p.file.Chmod(mode); err != nil {
			return nil, err
		}
	}
	p.fileInfo, err = p.file.Stat()
	if err != nil {
		return nil, err
	}
	p.output = filepath.Base(output)
	return p, nil
}

func sameRawObjectAt(dir *os.File, name string, info os.FileInfo) bool {
	var current unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return false
	}
	// Compare using the descriptor stat representation provided by os.File.
	stat := info.Sys().(*syscall.Stat_t)
	return uint64(current.Dev) == uint64(stat.Dev) && current.Ino == stat.Ino && current.Mode&unix.S_IFMT == stat.Mode&unix.S_IFMT
}

func (p *pendingRawOutput) verify() error {
	if !sameRawObjectAt(p.parent, p.name, p.dirInfo) || !sameRawObjectAt(p.dir, "image", p.fileInfo) {
		return errors.New("raw output temporary object was replaced")
	}
	return nil
}

func (p *pendingRawOutput) commit() error {
	if err := p.verify(); err != nil {
		return err
	}
	var current unix.Stat_t
	err := unix.Fstatat(int(p.parent.Fd()), p.output, &current, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err == nil && current.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("destination is not a regular file: %s", p.output)
	}
	return unix.Renameat(int(p.dir.Fd()), "image", int(p.parent.Fd()), p.output)
}

func (p *pendingRawOutput) closeFile() error {
	if p.file == nil {
		return nil
	}
	err := p.file.Close()
	p.file = nil
	return err
}

func (p *pendingRawOutput) Close() error {
	err := p.closeFile()
	if p.dir != nil {
		// Never follow a replaced entry or recursively remove a substituted tree.
		if p.ownedFile {
			removeErr := unix.Unlinkat(int(p.dir.Fd()), "image", 0)
			if !errors.Is(removeErr, unix.ENOENT) {
				err = errors.Join(err, removeErr)
			}
		}
		if p.dirInfo != nil && sameRawObjectAt(p.parent, p.name, p.dirInfo) {
			err = errors.Join(err, unix.Unlinkat(int(p.parent.Fd()), p.name, unix.AT_REMOVEDIR))
		}
		err = errors.Join(err, p.dir.Close())
		p.dir = nil
	}
	if p.parent != nil {
		err = errors.Join(err, p.parent.Close())
		p.parent = nil
	}
	return err
}
