package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

type visibilityBackend struct {
	*fs.Store
	opened  chan struct{}
	aborted chan struct{}
}
type visibilityHandle struct {
	store.PutHandle
	backend *visibilityBackend
}

func (b *visibilityBackend) OpenPut(g store.Generation, p store.Partition, k store.ContentKey, size *int64) (store.PutHandle, error) {
	h, err := b.Store.OpenPut(g, p, k, size)
	if err != nil {
		return nil, err
	}
	b.opened <- struct{}{}
	return &visibilityHandle{h, b}, nil
}
func (h *visibilityHandle) Abort() error {
	err := h.PutHandle.Abort()
	h.backend.aborted <- struct{}{}
	return err
}

// Real RPCs remain concurrent: both fs handles must open before either upload
// completes. B replaces A's partial inode; A does not wait for B's completion or
// reconfirm the name before returning its own successful write result.
func TestConcurrentPutCompletionDoesNotConfirmReplacement(t *testing.T) {
	for _, abortReplacement := range []bool{false, true} {
		name := "both-complete"
		if abortReplacement {
			name = "replacement-revoked"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			backend := &visibilityBackend{Store: newFSStore(t, "G1"), opened: make(chan struct{}, 2), aborted: make(chan struct{}, 2)}
			cli, _ := startBufconnServer(t, Options{Backend: backend, VerifyKey: true})
			data := []byte("concurrent content-addressed RPC upload")
			key := store.ContentKey(sha256.Sum256(data))
			size := uint64(len(data))
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			open := func(c context.Context) pb.Store_PutClient {
				t.Helper()
				s, err := cli.Put(c)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Send(&pb.PutRequest{Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_CHUNK, Generation: "G1", Key: key[:], Size: &size}}}); err != nil {
					t.Fatal(err)
				}
				wait(backend.opened)
				return s
			}
			finish := func(s pb.Store_PutClient) {
				t.Helper()
				if err := s.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: data}}); err != nil {
					t.Fatal(err)
				}
				r, err := s.CloseAndRecv()
				if err != nil || r == nil || !r.IsNew {
					t.Fatalf("Put = %v, %v; want completed write", r, err)
				}
			}
			a := open(ctx)
			replacementCtx, cancelReplacement := context.WithCancel(ctx)
			defer cancelReplacement()
			b := open(replacementCtx)
			finish(a)
			if abortReplacement {
				cancelReplacement()
				_, _ = b.CloseAndRecv()
				wait(backend.aborted)
			} else {
				finish(b)
			}
			found, got, err := backend.Get(ctx, "G1", store.PartitionChunk, key)
			if err != nil || found == abortReplacement || (found && !bytes.Equal(got, data)) {
				t.Fatalf("final object = %v, %q, %v", found, got, err)
			}
			// An aborted replacement actually removes the only remaining name. The
			// successful RPC is not a promise that this revocation will repair itself.
		})
	}
}
