//go:build linux

package server

import (
	"context"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// finishResponseDelivery fences the final TCP close against unread input.
// Linux tcp_close sends RST when receive bytes remain, discarding queued output
// even after Write returned nil. Send FIN and wait for the outgoing sequence
// (including FIN) to be acknowledged first. This needs no peer-application EOF
// handshake: an idle pooled client still acknowledges data/FIN in its kernel.
//
// TIOCOUTQ includes unacknowledged bytes; SIOCOUTQNSD would be insufficient.
// The ticker is only an observation interval, never a grace-period deadline.
// Force cancellation/DrainTimeout closes c and interrupts this wait.
func finishResponseDelivery(ctx context.Context, c net.Conn) error {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tc.CloseWrite(); err != nil {
		return err
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var pending int
		var socketErr error
		err = raw.Control(func(fd uintptr) {
			pending, socketErr = unix.IoctlGetInt(int(fd), unix.TIOCOUTQ)
			if socketErr != nil || pending == 0 {
				return
			}
			// A reset peer can leave an unacknowledged sequence behind. Do not wait
			// forever for ACKs from a connection the kernel has already closed.
			info, e := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
			if e != nil {
				socketErr = e
				return
			}
			const tcpClose = 7
			if info.State == tcpClose {
				socketErr = net.ErrClosed
			}
		})
		if err != nil {
			return err
		}
		if socketErr != nil {
			return socketErr
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
