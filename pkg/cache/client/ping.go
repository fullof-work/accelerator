package client

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
)

// Ping performs one wire Ping on an independent connection, which it closes
// before returning. The caller's context bounds the entire dial/write/read
// exchange. It proves wire responsiveness, not object or backend availability.
func Ping(ctx context.Context, endpoint string) error {
	return pingEndpoint(ctx, endpoint, (&net.Dialer{}).DialContext)
}

func pingEndpoint(ctx context.Context, endpoint string, dial func(context.Context, string, string) (net.Conn, error)) error {
	network, addr := parseAddr(endpoint)
	raw, err := dial(ctx, network, addr)
	if err != nil {
		return fmt.Errorf("ping dial: %w", err)
	}
	defer raw.Close()
	pc := &PoolConn{conn: wire.NewConn(raw)}
	return pc.ping(ctx)
}

func (pc *PoolConn) ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	finish := pc.readOperation(ctx, 0)
	defer func() {
		// Join cancellation before clearing the deadline or returning this
		// connection to its pool; a late callback must not poison its next user.
		finish()
		_ = pc.conn.SetDeadline(time.Time{})
	}()
	if err := pc.conn.WriteRequest(&wire.Request{Opcode: wire.OpcodePing}); err != nil {
		return fmt.Errorf("ping write: %w", err)
	}
	resp, err := pc.conn.ReadResponse(nil)
	if err != nil {
		return fmt.Errorf("ping read: %w", err)
	}
	return consumePingResponse(resp)
}

// The current response frame has no request ID or echoed opcode. Its Ping
// contract is exactly HIT with no error text and no payload. Own and release
// even an invalid response's Blob, just as for a valid response.
func consumePingResponse(resp *wire.Response) error {
	var size int
	if resp.Value != nil {
		defer resp.Value.Release()
		size = len(resp.Value.Bytes())
	}
	if resp.Status != wire.StatusHit || resp.ErrMsg != "" || size != 0 {
		return fmt.Errorf("ping: invalid response (status=%d error=%q value=%d bytes)", resp.Status, resp.ErrMsg, size)
	}
	return nil
}
