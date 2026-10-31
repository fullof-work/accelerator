package fs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestCommitDoesNotConfirmReadVisibility(t *testing.T) {
	for _, observation := range []error{os.ErrNotExist, os.ErrPermission} {
		t.Run(observation.Error(), func(t *testing.T) {
			backend, _ := newTestStore(t)
			data := []byte("completed upload, independently visible metadata")
			key := contentKey(data)
			h, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, int64Pointer(int64(len(data))))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.Write(data); err != nil {
				t.Fatal(err)
			}
			calls := 0
			backend.ops.lstat = func(path string) (os.FileInfo, error) {
				calls++
				return nil, &os.PathError{Op: "lstat", Path: path, Err: observation}
			}
			isNew, err := h.Commit(key, key)
			backend.ops.lstat = os.Lstat
			if err != nil || !isNew || calls != 0 {
				t.Fatalf("Commit = %v, %v; post-write Lstat calls = %d", isNew, err, calls)
			}
			found, got, err := backend.Get(context.Background(), testGeneration, store.PartitionChunk, key)
			if err != nil || !found || !bytes.Equal(got, data) {
				t.Fatalf("unchanged local object = %v, %q, %v", found, got, err)
			}
		})
	}
}

// Separate Store instances share no coordination. B really unlinks A's inode,
// rather than merely returning stale metadata. A's successful upload does not
// promise that B will finish, or that the revocable name will remain present.
func TestCompletedWriteWithCrossStoreReplacement(t *testing.T) {
	for _, outcome := range []string{"commit", "abort-before", "abort-after", "digest-failure"} {
		t.Run(outcome, func(t *testing.T) {
			backend, root := newTestStore(t)
			other, err := New(Config{Root: root})
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("same content across independent writers")
			key := contentKey(data)
			size := int64(len(data))
			a, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Abort()
			b, err := other.OpenPut(testGeneration, store.PartitionChunk, key, &size)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Abort()
			identity, err := a.(*PutHandle).file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			current, err := os.Lstat(backend.objectPath(store.PartitionChunk, testGeneration, key))
			if err != nil {
				t.Fatal(err)
			}
			if os.SameFile(identity, current) {
				t.Fatal("fixture did not replace the in-flight inode")
			}
			if _, err = a.Write(data); err != nil {
				t.Fatal(err)
			}
			if outcome == "abort-before" {
				if err = b.Abort(); err != nil {
					t.Fatal(err)
				}
			}
			if isNew, err := a.Commit(key, key); err != nil || !isNew {
				t.Fatalf("A Commit = %v, %v", isNew, err)
			}
			switch outcome {
			case "commit":
				if _, err = b.Write(data); err != nil {
					t.Fatal(err)
				}
				if isNew, err := b.Commit(key, key); err != nil || !isNew {
					t.Fatalf("B Commit = %v, %v", isNew, err)
				}
			case "abort-after":
				if err = b.Abort(); err != nil {
					t.Fatal(err)
				}
			case "digest-failure":
				bad := bytes.Repeat([]byte{'x'}, len(data))
				if _, err = b.Write(bad); err != nil {
					t.Fatal(err)
				}
				if isNew, err := b.Commit(key, contentKey(bad)); isNew || !errors.Is(err, ErrKeyMismatch) {
					t.Fatalf("invalid B Commit = %v, %v", isNew, err)
				}
			}
			found, got, err := backend.Get(context.Background(), testGeneration, store.PartitionChunk, key)
			wantFound := outcome == "commit"
			if err != nil || found != wantFound || (found && !bytes.Equal(got, data)) {
				t.Fatalf("final object = %v, %q, %v; want found=%v", found, got, err, wantFound)
			}
			// Missing in the abort/failure cases is actual removal, not simulated lag:
			// there is no pending writer or mechanism here that will recreate the name.
		})
	}
}

func TestCommitStillFailsWhenParentSyncCannotOpenDirectory(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("completed bytes do not excuse directory sync failure")
	key := contentKey(data)
	h, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, int64Pointer(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Write(data); err != nil {
		t.Fatal(err)
	}
	path := backend.objectPath(store.PartitionChunk, testGeneration, key)
	parent := filepath.Dir(path)
	moved := parent + ".moved"
	if err = os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if isNew, err := h.Commit(key, key); isNew || err == nil || !strings.Contains(err.Error(), "sync object parent") {
		t.Fatalf("Commit = %v, %v", isNew, err)
	}
	// Failure cleanup must not search for or delete a file at an unowned name.
	got, err := os.ReadFile(filepath.Join(moved, filepath.Base(path)))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("moved object = %q, %v", got, err)
	}
}
