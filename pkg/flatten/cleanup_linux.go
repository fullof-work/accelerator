//go:build linux

package flatten

import (
	"errors"
	"io"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

// removeOwnedWork is only for a private preparation directory, after all owned
// readers and writers have stopped. Image directory modes must survive emission,
// but a non-root owner may need write/search permission to remove that tree.
// The ordinary fast path needs no additional scan. Permission recovery stays
// beneath an anchored root, visits directories before reading their children,
// and never follows symlink entries or changes caller-owned source directories.
func removeOwnedWork(work string) error {
	original := os.RemoveAll(work)
	if !errors.Is(original, os.ErrPermission) {
		return original
	}
	root, err := os.OpenRoot(work)
	if err != nil {
		return errors.Join(original, err)
	}
	// Keep cleanup memory bounded on wide read-only trees; WalkDir would
	// allocate a complete directory listing before removing any entries.
	var makeRemovable func(string) error
	makeRemovable = func(name string) (retErr error) {
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return nil
		}
		if err = root.Chmod(name, info.Mode()|0700); err != nil {
			return err
		}
		directory, err := root.OpenFile(name, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, directory.Close()) }()
		for {
			entries, err := directory.ReadDir(128)
			for _, entry := range entries {
				if entry.IsDir() {
					if childErr := makeRemovable(path.Join(name, entry.Name())); childErr != nil {
						return childErr
					}
				}
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	walkErr := makeRemovable(".")
	closeErr := root.Close()
	removeErr := os.RemoveAll(work)
	if walkErr != nil || closeErr != nil || removeErr != nil {
		return errors.Join(original, walkErr, closeErr, removeErr)
	}
	return nil
}
