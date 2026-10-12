//go:build linux

package flatten

import (
	"errors"
	"io/fs"
	"os"
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
	walkErr := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return root.Chmod(name, info.Mode()|0700)
	})
	closeErr := root.Close()
	removeErr := os.RemoveAll(work)
	if walkErr != nil || closeErr != nil || removeErr != nil {
		return errors.Join(original, walkErr, closeErr, removeErr)
	}
	return nil
}
