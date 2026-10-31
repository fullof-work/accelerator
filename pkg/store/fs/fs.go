// Package fs implements the filesystem store data plane.
//
// Object layout:
//
//	{root}/{partition}/{generation}/{hex[:2]}/{hex[2:4]}/{hex}
//
// OpenPut writes that final path directly with create-exclusive semantics.
// There is no upload staging directory and Commit never renames an object.
package fs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

var (
	// ErrKeyMismatch reports that the streamed digest does not match the
	// content key bound by OpenPut.
	ErrKeyMismatch = errors.New("fs: content key mismatch")
	// ErrCommitKeyMismatch reports that Commit was called with a key other
	// than the key bound by OpenPut.
	ErrCommitKeyMismatch = errors.New("fs: commit key differs from open key")
	// ErrDirectIOUnsupported is returned by a direct-I/O Get on a platform
	// that cannot provide the required strict semantics.
	ErrDirectIOUnsupported = errors.New("fs: direct I/O unsupported")
)

// Config controls the filesystem data plane. Generation metadata is managed
// independently by store-ctl's configured generation source.
type Config struct {
	Root     string
	DirectIO bool
}

type ownedFile interface {
	io.Writer
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

type fileOps struct {
	lstat         func(string) (os.FileInfo, error)
	remove        func(string) error
	mkdirAll      func(string, os.FileMode) error
	openExclusive func(string, os.FileMode) (ownedFile, error)
}

// Store operates only on explicit generations. The optional hooks are kept
// per Store (never global) so concurrency tests can force narrow pathname
// races without serialising production operations.
type Store struct {
	root     string
	directIO bool
	ops      fileOps

	beforeMismatchRemove func(string)
	beforeExclusiveOpen  func(string)
	afterExclusiveEEXIST func(string)
}

// New opens an existing object root. It does not read generation metadata.
func New(cfg Config) (*Store, error) {
	if cfg.Root == "" {
		return nil, errors.New("fs: root is required")
	}
	info, err := os.Stat(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("fs: stat root %s: %w", cfg.Root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("fs: root %s is not a directory", cfg.Root)
	}
	return &Store{
		root:     cfg.Root,
		directIO: cfg.DirectIO,
		ops: fileOps{
			lstat:    os.Lstat,
			remove:   os.Remove,
			mkdirAll: os.MkdirAll,
			openExclusive: func(path string, mode os.FileMode) (ownedFile, error) {
				return openExclusiveNoFollow(path, mode)
			},
		},
	}, nil
}

type sizeExpectation struct {
	set   bool
	value int64
}

func copyExpectedSize(expected *int64) (sizeExpectation, error) {
	if expected == nil {
		return sizeExpectation{}, nil
	}
	if *expected < 0 {
		return sizeExpectation{}, fmt.Errorf("fs: negative expected size %d", *expected)
	}
	return sizeExpectation{set: true, value: *expected}, nil
}

func validateGeneration(generation store.Generation) error {
	if err := store.ValidateGeneration(generation); err != nil {
		return fmt.Errorf("fs: %w", err)
	}
	return nil
}

// Get reads one object from exactly the requested generation.
func (s *Store) Get(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	if err := validateGeneration(generation); err != nil {
		return false, nil, err
	}
	path := s.objectPath(partition, generation, key)
	var data []byte
	var err error
	if s.directIO {
		data, err = readDirectFile(path)
	} else {
		data, err = readBufferedFile(path)
	}
	if err == nil {
		return true, data, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil, nil
	}
	return false, nil, fmt.Errorf("fs: read %s: %w", path, err)
}

func readBufferedFile(path string) ([]byte, error) {
	f, err := openReadNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("object is not a regular file (mode %s)", info.Mode())
	}
	return io.ReadAll(f)
}

// Exists checks one final path. Only NotExist is a miss; symlinks,
// directories, special files, permission failures, and I/O failures are
// returned as errors.
func (s *Store) Exists(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateGeneration(generation); err != nil {
		return false, err
	}
	expected, err := copyExpectedSize(expectedSize)
	if err != nil {
		return false, err
	}
	_, exists, valid, err := s.inspectTarget(s.objectPath(partition, generation, key), expected)
	if err != nil {
		return false, err
	}
	return exists && valid, nil
}

// inspectTarget returns the lstat identity, path existence, and validity
// under the optional-size rule.
func (s *Store) inspectTarget(path string, expected sizeExpectation) (os.FileInfo, bool, bool, error) {
	info, err := s.ops.lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, false, nil
		}
		return nil, false, false, fmt.Errorf("fs: lstat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return info, true, false, fmt.Errorf("fs: target %s is not a regular file (mode %s)", path, info.Mode())
	}
	if expected.set && info.Size() != expected.value {
		return info, true, false, nil
	}
	return info, true, true, nil
}

// OpenPut binds generation, partition, key, and a copied expected size. It
// either returns a no-op handle for an already-valid target or creates the
// final file with O_EXCL and records its identity.
func (s *Store) OpenPut(generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (store.PutHandle, error) {
	if err := validateGeneration(generation); err != nil {
		return nil, err
	}
	expected, err := copyExpectedSize(expectedSize)
	if err != nil {
		return nil, err
	}
	finalPath := s.objectPath(partition, generation, key)
	if err := s.ops.mkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return nil, fmt.Errorf("fs: create object parent: %w", err)
	}

	for {
		info, exists, valid, err := s.inspectTarget(finalPath, expected)
		if err != nil {
			return nil, err
		}
		if exists && valid {
			return &noopPutHandle{key: key, expected: expected}, nil
		}
		if exists { // regular file with a confirmed size mismatch
			if s.beforeMismatchRemove != nil {
				s.beforeMismatchRemove(finalPath)
			}
			current, statErr := s.ops.lstat(finalPath)
			if statErr != nil {
				if errors.Is(statErr, fs.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("fs: recheck mismatched target %s: %w", finalPath, statErr)
			}
			if !os.SameFile(info, current) {
				continue
			}
			if err := s.ops.remove(finalPath); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("fs: remove mismatched target %s: %w", finalPath, err)
			}
		}

		if s.beforeExclusiveOpen != nil {
			s.beforeExclusiveOpen(finalPath)
		}
		f, err := s.ops.openExclusive(finalPath, 0o644)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				if s.afterExclusiveEEXIST != nil {
					s.afterExclusiveEEXIST(finalPath)
				}
				continue
			}
			return nil, fmt.Errorf("fs: create final object %s: %w", finalPath, err)
		}
		identity, err := f.Stat()
		if err != nil {
			_ = f.Close()
			// Without an fd identity we cannot prove that finalPath still
			// names the file created above. Leave any pathname cleanup to a
			// later size-aware Put instead of risking another writer's file.
			return nil, fmt.Errorf("fs: stat created object %s: %w", finalPath, err)
		}
		if !identity.Mode().IsRegular() {
			_ = f.Close()
			cleanupErr := s.removeIfSame(finalPath, identity)
			return nil, errors.Join(fmt.Errorf("fs: created object %s is not regular", finalPath), cleanupErr)
		}
		return &PutHandle{
			store:      s,
			file:       f,
			finalPath:  finalPath,
			identity:   identity,
			generation: generation,
			partition:  partition,
			key:        key,
			expected:   expected,
		}, nil
	}
}

// PutHandle writes directly to the final object inode.
type PutHandle struct {
	store      *Store
	file       ownedFile
	finalPath  string
	identity   os.FileInfo
	generation store.Generation
	partition  store.Partition
	key        store.ContentKey
	expected   sizeExpectation
	written    int64
	terminated bool
}

func (h *PutHandle) Write(p []byte) (int, error) {
	if h.terminated {
		return 0, errors.New("fs: write after commit/abort")
	}
	if h.written > math.MaxInt64-int64(len(p)) {
		return 0, errors.New("fs: written size overflow")
	}
	if h.expected.set && int64(len(p)) > h.expected.value-h.written {
		return 0, fmt.Errorf("fs: write exceeds expected size %d", h.expected.value)
	}
	n, err := h.file.Write(p)
	h.written += int64(n)
	return n, err
}

func (h *PutHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.terminated {
		return false, errors.New("fs: commit on terminated handle")
	}
	h.terminated = true
	if key != h.key {
		return false, h.fail(ErrCommitKeyMismatch)
	}
	if h.expected.set && h.written != h.expected.value {
		return false, h.fail(fmt.Errorf("fs: written size %d, expected %d", h.written, h.expected.value))
	}
	if verifyDigest != h.key {
		return false, h.fail(ErrKeyMismatch)
	}
	if err := h.file.Sync(); err != nil {
		return false, h.fail(fmt.Errorf("fs: sync final object: %w", err))
	}
	if err := h.file.Close(); err != nil {
		h.file = nil
		return false, h.fail(fmt.Errorf("fs: close final object: %w", err))
	}
	h.file = nil
	if err := syncDirectory(filepath.Dir(h.finalPath)); err != nil {
		return false, h.fail(fmt.Errorf("fs: sync object parent: %w", err))
	}

	// Completion belongs to this upload, not to a read-after-write observation
	// of the shared final path. Another same-key writer may replace that path,
	// and remote/shared filesystems need not expose current metadata immediately.
	// Pre-upload dedup handles return false; this handle completed a real write.
	return true, nil
}

func (h *PutHandle) fail(primary error) error {
	var cleanup []error
	if h.file != nil {
		if err := h.file.Close(); err != nil {
			cleanup = append(cleanup, fmt.Errorf("close owned file: %w", err))
		}
		h.file = nil
	}
	if err := h.removeOwned(); err != nil {
		cleanup = append(cleanup, err)
	}
	return errors.Join(append([]error{primary}, cleanup...)...)
}

func (h *PutHandle) removeOwned() error {
	if err := h.store.removeIfSame(h.finalPath, h.identity); err != nil {
		return fmt.Errorf("fs: remove owned path %s: %w", h.finalPath, err)
	}
	return nil
}

// removeIfSame removes path only after confirming that it still identifies
// the inode captured by the caller. A changed or already-absent path belongs
// to the concurrent winner and is left untouched.
func (s *Store) removeIfSame(path string, identity os.FileInfo) error {
	current, err := s.ops.lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("inspect path: %w", err)
	}
	if !os.SameFile(identity, current) {
		return nil
	}
	if err := s.ops.remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove path: %w", err)
	}
	return nil
}

func (h *PutHandle) Abort() error {
	if h.terminated {
		return nil
	}
	h.terminated = true
	var errs []error
	if h.file != nil {
		if err := h.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("fs: close aborted object: %w", err))
		}
		h.file = nil
	}
	if err := h.removeOwned(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

type noopPutHandle struct {
	key        store.ContentKey
	expected   sizeExpectation
	written    int64
	terminated bool
}

func (h *noopPutHandle) Write(p []byte) (int, error) {
	if h.terminated {
		return 0, errors.New("fs: no-op write after commit/abort")
	}
	if h.expected.set && int64(len(p)) > h.expected.value-h.written {
		return 0, fmt.Errorf("fs: no-op write exceeds expected size %d", h.expected.value)
	}
	if h.written > math.MaxInt64-int64(len(p)) {
		return 0, errors.New("fs: no-op written size overflow")
	}
	h.written += int64(len(p))
	return len(p), nil
}

func (h *noopPutHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.terminated {
		return false, errors.New("fs: no-op commit on terminated handle")
	}
	h.terminated = true
	if key != h.key {
		return false, ErrCommitKeyMismatch
	}
	if h.expected.set && h.written != h.expected.value {
		return false, fmt.Errorf("fs: no-op written size %d, expected %d", h.written, h.expected.value)
	}
	if verifyDigest != h.key {
		return false, ErrKeyMismatch
	}
	return false, nil
}

func (h *noopPutHandle) Abort() error {
	h.terminated = true
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *Store) objectPath(partition store.Partition, generation store.Generation, key store.ContentKey) string {
	hexKey := hex.EncodeToString(key[:])
	return filepath.Join(s.root, string(partition), string(generation), hexKey[:2], hexKey[2:4], hexKey)
}
