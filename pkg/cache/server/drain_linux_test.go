//go:build linux

package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestResponseACKWaitCanBeCancelled(t *testing.T) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	peer, err := net.DialTCP("tcp", nil, l.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := peer.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	c, err := l.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetWriteBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(make([]byte, 128<<10)); err != nil {
		t.Fatal(err)
	}
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var pending int
	var socketErr error
	if err := raw.Control(func(fd uintptr) { pending, socketErr = unix.IoctlGetInt(int(fd), unix.TIOCOUTQ) }); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil || pending == 0 {
		t.Fatalf("fixture needs unacknowledged output: %d, %v", pending, socketErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- finishResponseDelivery(ctx, c) }()
	select {
	case err := <-done:
		t.Fatalf("ACK wait returned with unread output: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ACK wait ignored cancellation")
	}
}
