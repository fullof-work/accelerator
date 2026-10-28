package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type smallSendListener struct{ net.Listener }

func (l smallSendListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if c != nil {
		if s, ok := c.(interface{ SetWriteBuffer(int) error }); ok {
			_ = s.SetWriteBuffer(64 << 10)
		}
	}
	return c, err
}

func startDrainWire(t *testing.T, network string, getter cache.Getter) (*WireServer, net.Conn) {
	t.Helper()
	addr := "127.0.0.1:0"
	if network == "unix" {
		addr = filepath.Join(t.TempDir(), "wire.sock")
	}
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatal(err)
	}
	s := NewWireServer(NewCacheHandler(roTier{getter}, nil), 0, 0)
	served := make(chan error, 1)
	go func() { served <- s.Serve(smallSendListener{l}) }()
	c, err := net.Dial(network, l.Addr().String())
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() {
		c.Close()
		s.Stop()
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	return s, c
}

type drainBlob struct {
	data     []byte
	released *atomic.Int32
}

func (b *drainBlob) Bytes() []byte     { return b.data }
func (b *drainBlob) Clone() cache.Blob { panic("unexpected clone") }
func (b *drainBlob) Release()          { b.released.Add(1) }

func TestDrainPipelinedResponsesAndSlowReader(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var freed atomic.Int32
			payload := bytes.Repeat([]byte{0x7d}, 3<<20)
			getter := cacheGetterFunc(func(ctx context.Context, _ store.Partition, k store.ContentKey) (cache.CacheResult, cache.Blob, error) {
				once.Do(func() { close(entered) })
				<-release
				if ctx.Err() != nil {
					t.Error("drain cancelled an admitted handler")
				}
				data := bytes.Clone(payload)
				data[0] = k[0]
				return cache.CacheHit, &drainBlob{data: data, released: &freed}, nil
			})
			s, raw := startDrainWire(t, network, getter)
			// Ensure assertion failures release the borrowed backend before cleanup joins.
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			c := wire.NewConn(raw)
			const n = 4
			for i := 0; i < n; i++ {
				req := &wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}
				req.Hash[0] = byte(i)
				if err := c.WriteRequest(req); err != nil {
					t.Fatal(err)
				}
			}
			<-entered
			// Ping is processed by the in-flight control loop only after the preceding
			// frames have entered reqs/pending. This is an admission barrier, not a sleep.
			if err := c.WriteRequest(&wire.Request{Opcode: wire.OpcodePing}); err != nil {
				t.Fatal(err)
			}
			ping, err := c.ReadResponse(nil)
			if err != nil || ping.Status != wire.StatusHit || ping.Value != nil {
				t.Fatalf("admission Ping: %v %v", ping, err)
			}
			s.Drain()
			done := make(chan struct{})
			go func() { s.Wait(); close(done) }()
			releaseOnce.Do(func() { close(release) })
			time.Sleep(20 * time.Millisecond) // let the first large response fill the small send buffer
			if freed.Load() != 0 {
				t.Fatal("Blob released before a slow reader could receive the full response")
			}
			select {
			case <-done:
				t.Fatal("drain abandoned queued responses")
			default:
			}
			for i := 0; i < n; i++ {
				resp, err := c.ReadResponse(nil)
				if err != nil {
					t.Fatalf("response %d: %v", i, err)
				}
				if resp.Value == nil {
					t.Fatalf("response %d has no payload: %+v", i, resp)
				}
				got := resp.Value.Bytes()
				valid := resp.Status == wire.StatusHit && len(got) == len(payload) && got[0] == byte(i) && bytes.Equal(got[1:], payload[1:])
				resp.Value.Release()
				if !valid {
					t.Fatalf("response %d truncated/reordered/corrupt", i)
				}
			}
			if _, err := c.ReadResponse(nil); err != io.EOF {
				t.Fatalf("drain end = %v, want EOF", err)
			}
			<-done
			if freed.Load() != n {
				t.Fatalf("released %d Blobs, want %d", freed.Load(), n)
			}
		})
	}
}

func TestHalfClosePreservesAdmittedHandler(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			s, raw := startDrainWire(t, network, cacheGetterFunc(func(ctx context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
				close(entered)
				<-release
				if ctx.Err() != nil {
					t.Error("peer write-half close cancelled handler")
				}
				return cache.CacheHit, cache.NewMemBlob([]byte("complete")), nil
			}))
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			c := wire.NewConn(raw)
			if err := c.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSBlob}); err != nil {
				t.Fatal(err)
			}
			<-entered
			if err := raw.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			once.Do(func() { close(release) })
			resp, err := c.ReadResponse(nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Value == nil {
				t.Fatal("lost response")
			}
			defer resp.Value.Release()
			if resp.Status != wire.StatusHit || string(resp.Value.Bytes()) != "complete" {
				t.Fatal("bad response")
			}
			s.GracefulStop()
		})
	}
}

func TestDrainDiscardsIncompleteInputWithoutCancellingResponse(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			payload := bytes.Repeat([]byte{0x4a}, 512<<10)
			s, raw := startDrainWire(t, network, cacheGetterFunc(func(ctx context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
				close(entered)
				<-release
				if ctx.Err() != nil {
					t.Error("local reader drain cancelled handler")
				}
				return cache.CacheHit, cache.NewMemBlob(payload), nil
			}))
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			c := wire.NewConn(raw)
			if err := c.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}); err != nil {
				t.Fatal(err)
			}
			<-entered
			partial := make([]byte, wire.RequestHeaderSize+8192)
			binary.LittleEndian.PutUint32(partial, 3<<20)
			partial[4] = wire.OpcodeObjectPut
			partial[5] = wire.NSChunk
			if _, err := raw.Write(partial); err != nil {
				t.Fatal(err)
			}
			s.Drain()
			once.Do(func() { close(release) })
			resp, err := c.ReadResponse(nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Value == nil {
				t.Fatal("lost response")
			}
			defer resp.Value.Release()
			if resp.Status != wire.StatusHit || !bytes.Equal(resp.Value.Bytes(), payload) {
				t.Fatal("truncated response")
			}
			s.Wait()
		})
	}
}

type slowDrainReader struct{ io.Reader }

func (r slowDrainReader) Read(p []byte) (int, error) {
	if len(p) > 4096 {
		p = p[:4096]
	}
	time.Sleep(time.Millisecond)
	return r.Reader.Read(p)
}

// A complete WriteResponse is not enough on Linux TCP: unread client input
// makes close send RST, which can discard the response's still-unacked tail.
func TestDrainSlowReaderWithUnadmittedInput(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			payload := bytes.Repeat([]byte{0x62}, 3<<20)
			s, raw := startDrainWire(t, network, cacheGetterFunc(func(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
				close(entered)
				<-release
				return cache.CacheHit, cache.NewMemBlob(payload), nil
			}))
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			if tc, ok := raw.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(64 << 10)
			}
			if err := wire.WriteRequest(raw, &wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}); err != nil {
				t.Fatal(err)
			}
			<-entered
			s.Drain()
			if _, err := raw.Write(make([]byte, 32<<10)); err != nil {
				t.Fatal(err)
			}
			wrote := make(chan struct{})
			go func() { defer close(wrote); _, _ = raw.Write(make([]byte, 2<<20)) }()
			t.Cleanup(func() { raw.Close(); <-wrote })
			once.Do(func() { close(release) })
			resp, err := wire.ReadResponse(slowDrainReader{raw}, nil)
			if err != nil {
				t.Fatalf("admitted response tail lost: %v", err)
			}
			if resp.Value == nil {
				t.Fatal("lost response")
			}
			defer resp.Value.Release()
			if resp.Status != wire.StatusHit || !bytes.Equal(resp.Value.Bytes(), payload) {
				t.Fatal("response truncated/corrupt")
			}
			// The idle/pool-owning caller need not send EOF for Run/Wait to finish.
			finished := make(chan struct{})
			go func() { s.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("drain requires a new peer EOF handshake")
			}
		})
	}
}

func TestForceStopJoinsBlockedResponseAndReleasesBlob(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			var freed atomic.Int32
			s, raw := startDrainWire(t, network, cacheGetterFunc(func(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
				return cache.CacheHit, &drainBlob{data: make([]byte, 3<<20), released: &freed}, nil
			}))
			if err := wire.WriteRequest(raw, &wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}); err != nil {
				t.Fatal(err)
			}
			// Receiving the header proves the handler has reached response delivery.
			header := make([]byte, wire.ResponseHeaderSize)
			if _, err := io.ReadFull(raw, header); err != nil {
				t.Fatal(err)
			}
			s.Drain()
			if freed.Load() != 0 {
				t.Fatal("large response unexpectedly fit the send buffer")
			}
			done := make(chan struct{})
			go func() { s.Stop(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("force stop did not join writer")
			}
			if freed.Load() != 1 {
				t.Fatalf("Blob releases=%d", freed.Load())
			}
			if _, err := wire.ReadResponse(io.MultiReader(bytes.NewReader(header), raw), nil); err == nil {
				t.Fatal("interrupted response reported success")
			}
		})
	}
}
