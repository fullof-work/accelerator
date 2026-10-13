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
// another object at commit or cleanup. No extra raw output copy is used.
type pendingRawOutput struct {
	file, dir, parent  *os.File
	fileInfo, dirInfo  os.FileInfo
	path, name, output string
	ownedFile          bool
	createdDir         *unix.Stat_t
}

func reserveRawOutput(output string) (_ *pendingRawOutput, retErr error) {
	mode, previous, err := outputPermissions(output)
	if err != nil {
		return nil, err
	}
	var accessACL []byte
	if previous != nil {
		accessACL, err = readOutputAccessACL(output)
		if err != nil {
			return nil, err
		}
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
		var created unix.Stat_t
		if err := unix.Fstatat(fd, name, &created, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return nil, err
		}
		if created.Mode&unix.S_IFMT != unix.S_IFDIR || created.Uid != uint32(os.Geteuid()) || created.Mode&0077 != 0 {
			return nil, errors.New("output staging directory is not private")
		}
		p.createdDir = &created
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
	if st.Dev != p.createdDir.Dev || st.Ino != p.createdDir.Ino {
		return nil, errors.New("output staging directory was replaced")
	}
	// Keep inherited setgid while restoring owner access. A usable directory
	// descriptor works on Linux 6.1 even in a privileged empty root without /proc.
	if st.Mode&0700 != 0700 {
		mode := uint32(0700) | st.Mode&unix.S_ISGID
		accessFD, openErr := unix.Openat(dirFD, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr == nil {
			err = errors.Join(unix.Fchmod(accessFD, mode), unix.Close(accessFD))
		} else {
			err = unix.Fchmodat(dirFD, "", mode, unix.AT_EMPTY_PATH)
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
				fileMode := os.FileMode(0700)
				if mode&unix.S_ISGID != 0 {
					fileMode |= os.ModeSetgid
				}
				err = os.Chmod(fmt.Sprintf("/proc/self/fd/%d", dirFD), fileMode)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("restore private staging directory permissions: %w", err)
		}
		var restored unix.Stat_t
		if err := unix.Fstat(dirFD, &restored); err != nil {
			return nil, err
		}
		if restored.Gid != st.Gid || restored.Mode&02777 != mode {
			return nil, errors.New("staging directory lost its private permissions or inherited group")
		}
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
		// Drop an inherited ACL when the existing destination had none, then
		// restore its rwx bits and access ACL while we still own the file.
		// A caller with CAP_CHOWN need not also have CAP_FOWNER. Output
		// files do not retain setuid/setgid execution bits.
		if accessACL == nil {
			if err := unix.Fremovexattr(fileFD, outputAccessACL); err != nil && !absentAccessACL(err) {
				return nil, err
			}
		}
		if err := p.file.Chmod(mode); err != nil {
			return nil, err
		}
		if accessACL != nil {
			if err := unix.Fsetxattr(fileFD, outputAccessACL, accessACL, 0); err != nil {
				return nil, err
			}
		}
		if st.Uid != uint32(uid) || st.Gid != uint32(gid) {
			if err := p.file.Chown(uid, gid); err != nil {
				return nil, err
			}
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
		err = errors.Join(err, p.dir.Close())
		p.dir = nil
	}
	if p.parent != nil {
		// An open failure (for example EMFILE) still owns the created inode.
		// Compare its identity before unlinking; never remove a replacement.
		if p.createdDir != nil {
			var current unix.Stat_t
			if statErr := unix.Fstatat(int(p.parent.Fd()), p.name, &current, unix.AT_SYMLINK_NOFOLLOW); statErr == nil {
				if current.Dev == p.createdDir.Dev && current.Ino == p.createdDir.Ino && current.Mode&unix.S_IFMT == unix.S_IFDIR {
					err = errors.Join(err, unix.Unlinkat(int(p.parent.Fd()), p.name, unix.AT_REMOVEDIR))
				}
			} else if !errors.Is(statErr, unix.ENOENT) {
				err = errors.Join(err, statErr)
			}
		}
		err = errors.Join(err, p.parent.Close())
		p.parent = nil
	}
	return err
}

const outputAccessACL = "system.posix_acl_access"

func absentAccessACL(err error) bool {
	return errors.Is(err, unix.ENODATA) || errors.Is(err, unix.EOPNOTSUPP)
}

func readOutputAccessACL(path string) ([]byte, error) {
	// Linux limits a single xattr value to 64 KiB. One bounded read avoids an
	// allocation/retry loop if the caller changes the destination concurrently.
	value := make([]byte, 64<<10)
	n, err := unix.Lgetxattr(path, outputAccessACL, value)
	if absentAccessACL(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read destination access ACL: %w", err)
	}
	return value[:n], nil
}
