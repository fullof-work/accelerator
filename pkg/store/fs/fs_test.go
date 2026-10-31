package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const testGeneration store.Generation = "G1"

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	backend, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return backend, root
}

func contentKey(data []byte) store.ContentKey {
	return store.ContentKey(sha256.Sum256(data))
}

func int64Pointer(value int64) *int64 { return &value }

func writeObject(t *testing.T, backend *Store, generation store.Generation, partition store.Partition, key store.ContentKey, data []byte, expected *int64, verify bool) bool {
	t.Helper()
	handle, err := backend.OpenPut(generation, partition, key, expected)
	if err != nil {
		t.Fatalf("OpenPut: %v", err)
	}
	if _, err := handle.Write(data); err != nil {
		_ = handle.Abort()
		t.Fatalf("Write: %v", err)
	}
	digest := key
	if verify {
		digest = contentKey(data)
	}
	isNew, err := handle.Commit(key, digest)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return isNew
}

func TestExplicitGenerationGetAndExists(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("generation-bound object")
	key := contentKey(data)
	size := int64(len(data))
	if !writeObject(t, backend, "G1", store.PartitionChunk, key, data, &size, true) {
		t.Fatal("first write was not new")
	}

	for _, test := range []struct {
		name     string
		expected *int64
		want     bool
	}{
		{name: "unknown size", want: true},
		{name: "matching size", expected: int64Pointer(size), want: true},
		{name: "different size", expected: int64Pointer(size + 1), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			exists, err := backend.Exists(context.Background(), "G1", store.PartitionChunk, key, test.expected)
			if err != nil || exists != test.want {
				t.Fatalf("Exists = %v, %v; want %v, nil", exists, err, test.want)
			}
		})
	}
	if found, _, err := backend.Get(context.Background(), "G2", store.PartitionChunk, key); err != nil || found {
		t.Fatalf("Get G2 = %v, %v; want miss", found, err)
	}
	found, got, err := backend.Get(context.Background(), "G1", store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("Get G1 = %v, %q, %v", found, got, err)
	}
}

func TestOpenPutNoopAndMismatchedReplacement(t *testing.T) {
	backend, _ := newTestStore(t)
	key := contentKey([]byte("claimed payload"))
	original := []byte("bad")
	writeObject(t, backend, testGeneration, store.PartitionChunk, key, original, int64Pointer(int64(len(original))), false)

	replacement := []byte("correct-size")
	size := int64(len(replacement))
	if !writeObject(t, backend, testGeneration, store.PartitionChunk, key, replacement, &size, false) {
		t.Fatal("size-mismatch replacement was not new")
	}
	found, got, err := backend.Get(context.Background(), testGeneration, store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement Get = %v, %q, %v", found, got, err)
	}
	if writeObject(t, backend, testGeneration, store.PartitionChunk, key, bytes.Repeat([]byte{'x'}, len(replacement)), &size, false) {
		t.Fatal("same-size target should return no-op dedup")
	}
	if writeObject(t, backend, testGeneration, store.PartitionChunk, key, []byte("ignored without size"), nil, false) {
		t.Fatal("existing target without expected size should return no-op dedup")
	}
}

func TestOpenPutCopiesExpectedSize(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("four")
	key := contentKey(data)
	expected := int64(len(data))
	handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &expected)
	if err != nil {
		t.Fatal(err)
	}
	expected = 999
	if _, err := handle.Write(data); err != nil {
		t.Fatal(err)
	}
	if isNew, err := handle.Commit(key, key); err != nil || !isNew {
		t.Fatalf("Commit = %v, %v", isNew, err)
	}
}

func TestPutValidationFailuresCleanOwnedFile(t *testing.T) {
	for _, test := range []struct {
		name   string
		commit func(store.PutHandle, store.ContentKey) error
	}{
		{
			name: "commit key mismatch",
			commit: func(handle store.PutHandle, key store.ContentKey) error {
				other := contentKey([]byte("other"))
				_, err := handle.Commit(other, key)
				return err
			},
		},
		{
			name: "digest mismatch",
			commit: func(handle store.PutHandle, key store.ContentKey) error {
				_, err := handle.Commit(key, contentKey([]byte("other")))
				return err
			},
		},
		{
			name: "short payload",
			commit: func(handle store.PutHandle, key store.ContentKey) error {
				_, err := handle.Commit(key, key)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, _ := newTestStore(t)
			data := []byte("payload")
			key := contentKey(data)
			expected := int64(len(data) + 1)
			if test.name != "short payload" {
				expected = int64(len(data))
			}
			handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &expected)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := handle.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := test.commit(handle, key); err == nil {
				t.Fatal("Commit unexpectedly succeeded")
			}
			if _, err := os.Lstat(backend.objectPath(store.PartitionChunk, testGeneration, key)); !os.IsNotExist(err) {
				t.Fatalf("owned path survived failure: %v", err)
			}
		})
	}
}

func TestWriteOverExpectedSizeAndAbort(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("too long")
	key := contentKey(data)
	handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, int64Pointer(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(data); err == nil {
		t.Fatal("oversized Write succeeded")
	}
	if err := handle.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(backend.objectPath(store.PartitionChunk, testGeneration, key)); !os.IsNotExist(err) {
		t.Fatalf("aborted path survived: %v", err)
	}
}

func TestVerifyFalseUsesOnlySize(t *testing.T) {
	backend, _ := newTestStore(t)
	claimedKey := contentKey([]byte("right"))
	wrong := []byte("wrong") // same size
	size := int64(len(wrong))
	if !writeObject(t, backend, testGeneration, store.PartitionChunk, claimedKey, wrong, &size, false) {
		t.Fatal("verify=false write was not new")
	}
	if writeObject(t, backend, testGeneration, store.PartitionChunk, claimedKey, []byte("other"), &size, false) {
		t.Fatal("same-size wrong content should be indistinguishable and dedup")
	}

	unknownSizeKey := contentKey([]byte("another claimed value"))
	unknownSizePayload := []byte("unverified payload with no expected size")
	if !writeObject(t, backend, testGeneration, store.PartitionChunk, unknownSizeKey, unknownSizePayload, nil, false) {
		t.Fatal("verify=false unknown-size write was not new")
	}
	if writeObject(t, backend, testGeneration, store.PartitionChunk, unknownSizeKey, []byte("incomplete would still dedup"), nil, false) {
		t.Fatal("unknown-size existing target should use existence-only dedup")
	}
}

func TestConcurrentWritersSameKeyOwnership(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("concurrent final payload")
	key := contentKey(data)
	size := int64(len(data))
	handleA, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		t.Fatal(err)
	}
	half := len(data) / 2
	if _, err := handleA.Write(data[:half]); err != nil {
		t.Fatal(err)
	}

	opened := make(chan struct{})
	done := make(chan struct{})
	var (
		resultB bool
		errorB  error
	)
	go func() {
		handleB, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
		close(opened)
		if err == nil {
			_, err = handleB.Write(data)
		}
		if err == nil {
			resultB, err = handleB.Commit(key, key)
		}
		errorB = err
		close(done)
	}()
	<-opened
	if _, err := handleA.Write(data[half:]); err != nil {
		t.Fatal(err)
	}
	<-done
	if errorB != nil || !resultB {
		t.Fatalf("writer B = %v, %v; want new", resultB, errorB)
	}
	resultA, err := handleA.Commit(key, key)
	if err != nil || !resultA {
		t.Fatalf("writer A = %v, %v; want completed direct write", resultA, err)
	}
	found, got, err := backend.Get(context.Background(), testGeneration, store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("final object = %v, %q, %v", found, got, err)
	}
}

func TestCompletedWriteSucceedsWhileReplacementStillInFlight(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("complete payload")
	key := contentKey(data)
	size := int64(len(data))
	handleA, _ := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	_, _ = handleA.Write(data[:1])
	handleB, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = handleA.Write(data[1:])
	_, _ = handleB.Write(data[:1])
	if isNew, err := handleA.Commit(key, key); err != nil || !isNew {
		t.Fatalf("completed write Commit = %v, %v; want success independent of replacement", isNew, err)
	}
	if _, err := handleB.Write(data[1:]); err != nil {
		t.Fatal(err)
	}
	if isNew, err := handleB.Commit(key, key); err != nil || !isNew {
		t.Fatalf("winner Commit = %v, %v", isNew, err)
	}
}

func TestAbortDoesNotDeleteReplacement(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("replacement")
	key := contentKey(data)
	size := int64(len(data))
	handleA, _ := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	_, _ = handleA.Write(data[:1])
	handleB, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = handleB.Write(data)
	if _, err := handleB.Commit(key, key); err != nil {
		t.Fatal(err)
	}
	if err := handleA.Abort(); err != nil {
		t.Fatal(err)
	}
	found, got, err := backend.Get(context.Background(), testGeneration, store.PartitionChunk, key)
	if err != nil || !found || !bytes.Equal(got, data) {
		t.Fatalf("replacement removed: %v, %q, %v", found, got, err)
	}
}

func TestMismatchIdentityRecheckedBeforeDelete(t *testing.T) {
	backend, _ := newTestStore(t)
	key := contentKey([]byte("claimed"))
	path := backend.objectPath(store.PartitionChunk, testGeneration, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("valid")
	var once sync.Once
	backend.beforeMismatchRemove = func(path string) {
		once.Do(func() {
			replacementPath := path + ".replacement"
			if err := os.WriteFile(replacementPath, replacement, 0o644); err != nil {
				t.Errorf("prepare replacement: %v", err)
				return
			}
			if err := os.Remove(path); err != nil {
				t.Errorf("remove old: %v", err)
				return
			}
			if err := os.Link(replacementPath, path); err != nil {
				t.Errorf("install replacement: %v", err)
			}
			_ = os.Remove(replacementPath)
		})
	}
	size := int64(len(replacement))
	handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = handle.Write(replacement)
	if isNew, err := handle.Commit(key, key); err != nil || isNew {
		t.Fatalf("Commit = %v, %v; want dedup", isNew, err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, replacement) {
		t.Fatalf("replacement was deleted: %q", got)
	}
}

func TestExclusiveEEXISTRechecksTarget(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("race winner")
	key := contentKey(data)
	var once sync.Once
	backend.beforeExclusiveOpen = func(path string) {
		once.Do(func() {
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Errorf("create racing target: %v", err)
			}
		})
	}
	eexists := 0
	backend.afterExclusiveEEXIST = func(string) { eexists++ }
	size := int64(len(data))
	handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = handle.Write(data)
	if isNew, err := handle.Commit(key, key); err != nil || isNew {
		t.Fatalf("Commit = %v, %v; want dedup", isNew, err)
	}
	if eexists != 1 {
		t.Fatalf("EEXIST hook count = %d, want 1", eexists)
	}
}

func TestFailureDoesNotDeleteAnotherWritersResult(t *testing.T) {
	backend, _ := newTestStore(t)
	data := []byte("other writer result")
	key := contentKey(data)
	size := int64(len(data))
	handleA, _ := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	_, _ = handleA.Write(data)
	path := backend.objectPath(store.PartitionChunk, testGeneration, key)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := handleA.Commit(key, contentKey([]byte("bad digest"))); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("Commit error = %v, want ErrKeyMismatch", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("other result removed: %q, %v", got, err)
	}
}

func TestNonRegularTargetsAreErrors(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			backend, _ := newTestStore(t)
			key := contentKey([]byte(kind))
			path := backend.objectPath(store.PartitionChunk, testGeneration, key)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink":
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if exists, err := backend.Exists(context.Background(), testGeneration, store.PartitionChunk, key, nil); err == nil || exists {
				t.Fatalf("Exists = %v, %v; want error", exists, err)
			}
			if _, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, nil); err == nil {
				t.Fatal("OpenPut accepted non-regular target")
			}
		})
	}
}

func TestExistsPropagatesFilesystemErrors(t *testing.T) {
	for _, injected := range []error{syscall.EACCES, syscall.EIO, syscall.ESTALE} {
		t.Run(injected.Error(), func(t *testing.T) {
			backend, _ := newTestStore(t)
			backend.ops.lstat = func(string) (os.FileInfo, error) { return nil, injected }
			key := contentKey([]byte("error"))
			exists, err := backend.Exists(context.Background(), testGeneration, store.PartitionChunk, key, nil)
			if exists || !errors.Is(err, injected) {
				t.Fatalf("Exists = %v, %v; want false, %v", exists, err, injected)
			}
		})
	}
}

type failingFile struct {
	ownedFile
	writeErr error
	syncErr  error
	closeErr error
	closed   bool
}

func (f *failingFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.ownedFile.Write(p)
}

func (f *failingFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.ownedFile.Sync()
}

func (f *failingFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	err := f.ownedFile.Close()
	if f.closeErr != nil {
		return f.closeErr
	}
	return err
}

func TestWriteSyncCloseFailuresCleanOwnedPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		writeErr error
		syncErr  error
		closeErr error
	}{
		{name: "write", writeErr: errors.New("write failure")},
		{name: "sync", syncErr: errors.New("sync failure")},
		{name: "close", closeErr: errors.New("close failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, _ := newTestStore(t)
			open := backend.ops.openExclusive
			backend.ops.openExclusive = func(path string, mode os.FileMode) (ownedFile, error) {
				file, err := open(path, mode)
				if err != nil {
					return nil, err
				}
				return &failingFile{ownedFile: file, writeErr: test.writeErr, syncErr: test.syncErr, closeErr: test.closeErr}, nil
			}
			data := []byte("failure payload")
			key := contentKey(data)
			size := int64(len(data))
			handle, err := backend.OpenPut(testGeneration, store.PartitionChunk, key, &size)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := handle.Write(data)
			if test.writeErr != nil {
				if writeErr == nil {
					t.Fatal("Write succeeded")
				}
				_ = handle.Abort()
			} else {
				if writeErr != nil {
					t.Fatal(writeErr)
				}
				if _, err := handle.Commit(key, key); err == nil {
					t.Fatal("Commit succeeded")
				}
			}
			if _, err := os.Lstat(backend.objectPath(store.PartitionChunk, testGeneration, key)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("owned path survived: %v", err)
			}
		})
	}
}

func TestExplicitGenerationAdminHelpers(t *testing.T) {
	backend, root := newTestStore(t)
	for _, generation := range []store.Generation{"G1", "G2"} {
		data := []byte(generation)
		writeObject(t, backend, generation, store.PartitionBlob, contentKey(data), data, int64Pointer(int64(len(data))), true)
	}
	stats, err := backend.GenerationStats(context.Background(), "G1")
	if err != nil || stats[store.PartitionBlob] != 1 {
		t.Fatalf("GenerationStats = %v, %v", stats, err)
	}
	if err := backend.DropGeneration(context.Background(), "G1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "blob", "G2")); err != nil {
		t.Fatalf("G2 removed with G1: %v", err)
	}
	if err := backend.Wipe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "blob")); !os.IsNotExist(err) {
		t.Fatalf("partition survived Wipe: %v", err)
	}
}

func BenchmarkGetBuffered(b *testing.B) {
	benchmarkGet(b, false)
}

func BenchmarkGetDirectIO(b *testing.B) {
	benchmarkGet(b, true)
}

func benchmarkGet(b *testing.B, direct bool) {
	root := b.TempDir()
	writer, err := New(Config{Root: root})
	if err != nil {
		b.Fatal(err)
	}
	data := bytes.Repeat([]byte("benchmark"), 128*1024)
	key := contentKey(data)
	size := int64(len(data))
	handle, err := writer.OpenPut(testGeneration, store.PartitionChunk, key, &size)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := handle.Write(data); err != nil {
		b.Fatal(err)
	}
	if _, err := handle.Commit(key, key); err != nil {
		b.Fatal(err)
	}
	reader, err := New(Config{Root: root, DirectIO: direct})
	if err != nil {
		b.Fatal(err)
	}
	if direct {
		if _, _, err := reader.Get(context.Background(), testGeneration, store.PartitionChunk, key); errors.Is(err, ErrDirectIOUnsupported) {
			b.Skip(err)
		}
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		found, got, err := reader.Get(context.Background(), testGeneration, store.PartitionChunk, key)
		if err != nil || !found || len(got) != len(data) {
			b.Fatalf("Get = %v, %d, %v", found, len(got), err)
		}
	}
}
