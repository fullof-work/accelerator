package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type blockingGetter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *blockingGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	return cache.CacheMiss, nil, nil
}

type singleConnListener struct {
	conn   net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *singleConnListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (*singleConnListener) Addr() net.Addr { return testAddr("wire-test") }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

type acceptResult struct {
	conn net.Conn
	err  error
}

type scriptedListener struct {
	results chan acceptResult
	closed  chan struct{}
	once    sync.Once
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	select {
	case result := <-l.results:
		return result.conn, result.err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *scriptedListener) Close() error { l.once.Do(func() { close(l.closed) }); return nil }
func (*scriptedListener) Addr() net.Addr { return testAddr("scripted") }

type temporaryAcceptError struct{}

func (temporaryAcceptError) Error() string   { return "temporary accept failure" }
func (temporaryAcceptError) Timeout() bool   { return false }
func (temporaryAcceptError) Temporary() bool { return true }

func TestServeAfterImmediateStopClosesListener(t *testing.T) {
	for range 100 {
		listener := &scriptedListener{results: make(chan acceptResult), closed: make(chan struct{})}
		s := NewWireServer(NewCacheHandler(nil, nil), 0, 0)
		done := make(chan error, 1)
		go func() { done <- s.Serve(listener) }()
		s.GracefulStop()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		select {
		case <-listener.closed:
		default:
			t.Fatal("listener supplied after shutdown was not closed")
		}
	}
}

func TestServeReturnsPermanentAcceptError(t *testing.T) {
	want := errors.New("permanent accept failure")
	listener := &scriptedListener{results: make(chan acceptResult, 1), closed: make(chan struct{})}
	listener.results <- acceptResult{err: want}
	s := NewWireServer(NewCacheHandler(nil, nil), 0, 0)
	if err := s.Serve(listener); !errors.Is(err, want) {
		t.Fatalf("Serve error = %v, want %v", err, want)
	}
}

func TestServeRetriesTemporaryAcceptError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	listener := &scriptedListener{results: make(chan acceptResult, 2), closed: make(chan struct{})}
	listener.results <- acceptResult{err: temporaryAcceptError{}}
	listener.results <- acceptResult{conn: serverConn}
	s := NewWireServer(NewCacheHandler(nil, nil), 0, 0)
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	deadline := time.After(time.Second)
	for s.ConnCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("connection was not accepted after temporary error")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	s.GracefulStop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStopWaitsForUncancellableHandler(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	listener := &singleConnListener{conn: serverConn, closed: make(chan struct{})}
	getter := &blockingGetter{started: make(chan struct{}), release: make(chan struct{})}
	s := NewWireServer(NewCacheHandler(roTier{getter}, nil), 0, 0)
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(listener) }()

	client := wire.NewConn(clientConn)
	if err := client.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}); err != nil {
		t.Fatal(err)
	}
	<-getter.started

	waitDone := make(chan struct{})
	go func() { s.Stop(); close(waitDone) }()
	select {
	case <-waitDone:
		t.Fatal("Wait returned while the handler was still active")
	default:
	}

	close(getter.release)
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after the handler completed")
	}
	if err := <-serveDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Serve: %v", err)
	}
	_ = clientConn.Close()
}

func TestStartConnRejectsAdmissionAfterStop(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	s := NewWireServer(NewCacheHandler(nil, nil), 0, 0)
	s.GracefulStop()
	if s.startConn(serverConn) {
		t.Fatal("connection admitted after stop")
	}
	_ = serverConn.Close()
	_ = clientConn.Close()
	done := make(chan struct{})
	go func() {
		s.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rejected admission left Wait blocked")
	}
}

func TestWriteFailureReleasesBlockedReader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	listener := &singleConnListener{conn: serverConn, closed: make(chan struct{})}
	s := NewWireServer(NewCacheHandler(roTier{cacheGetterFunc(func(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
		return cache.CacheMiss, nil, nil
	})}, nil), 0, 0)
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(listener) }()
	client := wire.NewConn(clientConn)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for range 8 {
			if client.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}) != nil {
				return
			}
		}
	}()
	deadline := time.After(time.Second)
	for s.ConnCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("connection was not tracked")
		default:
		}
	}
	_ = clientConn.Close() // fail the response write and unblock the full reader
	s.GracefulStop()
	waitDone := make(chan struct{})
	go func() { s.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after connection shutdown")
	}
	<-writeDone
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

type cacheGetterFunc func(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error)

func (f cacheGetterFunc) Get(ctx context.Context, p store.Partition, k store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	return f(ctx, p, k)
}
