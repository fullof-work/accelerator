package s3

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type visibilityLagS3 struct {
	*fakeS3
	hide      bool
	postHeads int
	postGets  int
}

func (f *visibilityLagS3) Head(ctx context.Context, key string) (*ObjectMeta, error) {
	if f.hide {
		f.postHeads++
		return nil, ErrNotFound
	}
	return f.fakeS3.Head(ctx, key)
}
func (f *visibilityLagS3) Get(ctx context.Context, key string) ([]byte, *ObjectMeta, error) {
	if f.hide {
		f.postGets++
		return nil, nil, ErrNotFound
	}
	return f.fakeS3.Get(ctx, key)
}
func (f *visibilityLagS3) Put(ctx context.Context, key string, data []byte, opts PutOptions) (string, error) {
	etag, err := f.fakeS3.Put(ctx, key, data, opts)
	if err == nil {
		f.hide = true
	}
	return etag, err
}
func TestCommitDoesNotConfirmImmediateReadVisibility(t *testing.T) {
	fake := &visibilityLagS3{fakeS3: newFakeS3()}
	s, err := New(context.Background(), fake, Config{Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("s3 completed upload, reads temporarily stale")
	key := keyFromBytes(data)
	h, err := s.OpenPut("G1", store.PartitionChunk, key, sizePointer(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Write(data); err != nil {
		t.Fatal(err)
	}
	isNew, err := h.Commit(key, key)
	if err != nil || !isNew || fake.postHeads != 0 || fake.postGets != 0 {
		t.Fatalf("Commit=%v %v, post HEAD/GET=%d/%d", isNew, err, fake.postHeads, fake.postGets)
	}
	if found, _, err := s.Get(context.Background(), "G1", store.PartitionChunk, key); found || err != nil {
		t.Fatalf("modeled stale read=%v %v", found, err)
	}
	if err = h.Abort(); err != nil {
		t.Fatal(err)
	}
	fake.hide = false
	found, got, err := s.Get(context.Background(), "G1", store.PartitionChunk, key)
	if !found || err != nil || !bytes.Equal(got, data) {
		t.Fatalf("eventual read=%v %q %v", found, got, err)
	}
	// Clearing the test visibility fault reveals bytes already accepted by Put.
	// This does not promise that a genuinely removed object will reappear.
}
func TestCommitPreservesRemotePutFailure(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake)
	data := []byte("real write failure")
	key := keyFromBytes(data)
	h, err := s.OpenPut("G1", store.PartitionChunk, key, sizePointer(int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Write(data); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("remote write failure")
	fake.failNextPut = failure
	if n, err := h.Commit(key, key); n || !errors.Is(err, failure) {
		t.Fatalf("Commit=%v,%v", n, err)
	}
}
