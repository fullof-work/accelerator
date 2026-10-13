//go:build linux

package flatten

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

// A selection is fixed before invoking native mkfs and used unchanged for the
// final validation. It detects changes; it is not an atomic filesystem snapshot.
type sourceSelection struct {
	root     string
	excludes []string
	stamp    [32]byte
	budget   uint64
	owned    []ownedPath
}

type ownedPath struct {
	dev, ino             uint64
	parentDev, parentIno uint64
	leaf                 string
}

func (s *sourceSelection) excluded(name string) bool {
	for p := name; p != "."; p = filepath.Dir(p) {
		i := sort.SearchStrings(s.excludes, p)
		if i < len(s.excludes) && s.excludes[i] == p {
			return true
		}
	}
	return false
}

func (s *sourceSelection) snapshot(ctx context.Context) ([32]byte, error) {
	h := sha256.New()
	remaining := s.budget
	err := s.walk(ctx, ".", h, &remaining)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, err
}

func (s *sourceSelection) validate(ctx context.Context) error {
	got, err := s.snapshot(ctx)
	if err != nil {
		return err
	}
	if got != s.stamp {
		return fmt.Errorf("flatten: selected source tree changed")
	}
	return nil
}

func (s *sourceSelection) walk(ctx context.Context, rel string, h hash.Hash, remaining *uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rel != "." && s.excluded(rel) {
		return nil
	}
	charge := uint64(512 + 4*len(rel))
	if len(rel) > 4096 || charge > *remaining {
		return fmt.Errorf("flatten: source selection budget exceeded")
	}
	*remaining -= charge
	name := filepath.Join(s.root, rel)
	var st unix.Stat_t
	if err := unix.Lstat(name, &st); err != nil {
		return err
	}
	id := statID(&st)
	if rel != "." && len(s.owned) != 0 {
		var parent unix.Stat_t
		parentRead := false
		for _, owned := range s.owned {
			match := owned.ino != 0 && id.dev == owned.dev && id.ino == owned.ino
			if !match && filepath.Base(rel) == owned.leaf {
				if !parentRead {
					if err := unix.Stat(filepath.Dir(name), &parent); err != nil {
						return err
					}
					parentRead = true
				}
				match = uint64(parent.Dev) == owned.parentDev && parent.Ino == owned.parentIno
			}
			if match {
				s.excludes = dedupeSorted(append(s.excludes, rel))
				return nil
			}
		}
	}
	if st.Mode&unix.S_IFMT == unix.S_IFDIR {
		// Directory size/link count and times include excluded children. Their
		// exported values are derived by mkfs from the selected entries, and
		// output times are fixed. Hash selected children and identity/mode/owner.
		id.size, id.nlink, id.mtime, id.ctime, id.mtimeNS, id.ctimeNS = 0, 0, 0, 0, 0, 0
	}
	h.Write([]byte(rel))
	h.Write([]byte{0})
	// Serialize the same identity once, avoiding twelve tiny hash writes and
	// escaping buffers per inode on wide source trees.
	var encoded [12 * 8]byte
	for i, n := range [...]uint64{id.dev, id.ino, id.size, id.nlink, uint64(id.mtime), uint64(id.ctime), uint64(id.mtimeNS), uint64(id.ctimeNS), uint64(id.mode), uint64(id.uid), uint64(id.gid), uint64(st.Rdev)} {
		binary.LittleEndian.PutUint64(encoded[i*8:], n)
	}
	h.Write(encoded[:])
	if st.Mode&unix.S_IFMT == unix.S_IFLNK {
		target, err := os.Readlink(name)
		if err != nil {
			return err
		}
		h.Write([]byte(target))
		h.Write([]byte{0})
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil
	}
	d, err := os.Open(name)
	if err != nil {
		return err
	}
	var entries []string
	for {
		batch, e := d.ReadDir(256)
		for _, entry := range batch {
			r := filepath.Join(rel, entry.Name())
			if s.excluded(r) {
				continue
			}
			// Bound a wide directory before collecting/sorting its entry names.
			cost := uint64(128 + 2*len(entry.Name()))
			if cost > *remaining {
				d.Close()
				return fmt.Errorf("flatten: directory index budget exceeded")
			}
			*remaining -= cost
			entries = append(entries, entry.Name())
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			d.Close()
			return e
		}
		if err := ctx.Err(); err != nil {
			d.Close()
			return err
		}
	}
	if err := d.Close(); err != nil {
		return err
	}
	sort.Strings(entries)
	for _, entry := range entries {
		if err := s.walk(ctx, filepath.Join(rel, entry), h, remaining); err != nil {
			return err
		}
	}
	return nil
}

func selectSource(ctx context.Context, root string, excludes []string, budget uint64) (*sourceSelection, error) {
	return selectSourceWithOwned(ctx, root, excludes, nil, nil, budget)
}

func selectSourceWithOwned(ctx context.Context, root string, excludes, ownedPaths []string, outputFiles []*os.File, budget uint64) (*sourceSelection, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("flatten: source is not a directory")
	}
	s := &sourceSelection{root: abs, excludes: dedupeSorted(append([]string(nil), excludes...)), budget: budget}
	for _, p := range s.excludes {
		if !validPlanPath(p) {
			return nil, fmt.Errorf("flatten: invalid source exclusion %q", p)
		}
	}
	for _, name := range ownedPaths {
		name, err = filepath.Abs(name)
		if err != nil {
			return nil, err
		}
		parent, err := filepath.EvalSymlinks(filepath.Dir(name))
		if err != nil {
			return nil, err
		}
		var parentStat, st unix.Stat_t
		if err = unix.Stat(parent, &parentStat); err != nil {
			return nil, err
		}
		owned := ownedPath{parentDev: uint64(parentStat.Dev), parentIno: parentStat.Ino, leaf: filepath.Base(name)}
		if err = unix.Lstat(name, &st); err == nil {
			owned.dev, owned.ino = uint64(st.Dev), st.Ino
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		if owned.ino != 0 {
			var rootStat unix.Stat_t
			if err = unix.Stat(abs, &rootStat); err != nil {
				return nil, err
			}
			if rootStat.Ino == owned.ino && uint64(rootStat.Dev) == owned.dev {
				return nil, fmt.Errorf("flatten: output or scratch is the source root")
			}
		}
		if rel, ok := strictlyUnder(abs, filepath.Join(parent, owned.leaf)); ok {
			s.excludes = dedupeSorted(append(s.excludes, rel))
		}
		s.owned = append(s.owned, owned)
	}
	for _, file := range outputFiles {
		if file == nil {
			return nil, fmt.Errorf("flatten: nil output file")
		}
		var st unix.Stat_t
		if err := unix.Fstat(int(file.Fd()), &st); err != nil {
			return nil, fmt.Errorf("flatten: stat output file: %w", err)
		}
		if st.Mode&unix.S_IFMT == unix.S_IFREG {
			s.owned = append(s.owned, ownedPath{dev: uint64(st.Dev), ino: st.Ino})
		}
	}
	s.stamp, err = s.snapshot(ctx)
	// The discovered aliases become part of the fixed native/read/validate
	// selection; final validation never grows a fresh set of exclusions.
	s.owned = nil
	return s, err
}
