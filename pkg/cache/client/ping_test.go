package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
)

func pingListener(t *testing.T, network string) (net.Listener, string) {
	t.Helper()
	addr := "127.0.0.1:0"
	if network == "unix" {
		addr = filepath.Join(t.TempDir(), "peer.sock")
	}
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	endpoint := l.Addr().String()
	if network == "unix" {
		endpoint = "unix://" + endpoint
	}
	return l, endpoint
}

func TestPingResponses(t *testing.T) {
	cases := []struct {
		name    string
		resp    wire.Response
		partial bool
		wantOK  bool
	}{
		{name: "hit", resp: wire.Response{Status: wire.StatusHit}, wantOK: true},
		{name: "miss", resp: wire.Response{Status: wire.StatusMiss}},
		{name: "cancelled", resp: wire.Response{Status: wire.StatusCancelled}},
		{name: "error", resp: wire.Response{Status: wire.StatusError, ErrMsg: "unavailable"}},
		{name: "unknown", resp: wire.Response{Status: 255}},
		{name: "hit-error-text", resp: wire.Response{Status: wire.StatusHit, ErrMsg: "bad"}},
		{name: "hit-payload", resp: wire.Response{Status: wire.StatusHit, Value: cache.NewMemBlob([]byte("unexpected"))}},
		{name: "error-payload", resp: wire.Response{Status: wire.StatusError, Value: cache.NewMemBlob([]byte("unexpected"))}},
		{name: "half-frame", partial: true},
	}
	for _, network := range []string{"tcp", "unix"} {
		for _, pooled := range []bool{false, true} {
			mode := "short"
			if pooled {
				mode = "pool"
			}
			for _, tc := range cases {
				t.Run(network+"/"+mode+"/"+tc.name, func(t *testing.T) {
					l, endpoint := pingListener(t, network)
					done := make(chan error, 1)
					go func() {
						c, err := l.Accept()
						if err != nil {
							done <- err
							return
						}
						defer c.Close()
						c.SetDeadline(time.Now().Add(2 * time.Second))
						req, err := wire.ReadRequest(c)
						if err != nil {
							done <- err
							return
						}
						defer req.Release()
						if req.Opcode != wire.OpcodePing {
							done <- errors.New("not a Ping request")
							return
						}
						if tc.partial {
							_, err = c.Write([]byte{8, 0, 0, 0})
							done <- err
							return
						}
						if err = wire.WriteResponse(c, &tc.resp); err != nil {
							done <- err
							return
						}
						var b [1]byte
						_, err = c.Read(b[:])
						if err != io.EOF {
							done <- errors.New("probe did not close its connection")
							return
						}
						done <- nil
					}()
					var err error
					if pooled {
						p, e := DialConnPool(endpoint, ConnPoolConfig{PoolSize: 1, MaxSize: 1, Timeout: time.Second})
						if e != nil {
							t.Fatal(e)
						}
						pc, e := p.Acquire(context.Background())
						if e != nil {
							p.Close()
							t.Fatal(e)
						}
						err = pc.Ping(time.Second)
						p.Release(pc, false)
						p.Close()
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						err = Ping(ctx, endpoint)
						cancel()
					}
					if (err == nil) != tc.wantOK {
						t.Errorf("Ping error = %v, wantOK = %v", err, tc.wantOK)
					}
					l.Close()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestPingListenerOnly(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			_, endpoint := pingListener(t, network)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if err := Ping(ctx, endpoint); err == nil {
				t.Fatal("unaccepted socket reported healthy")
			}
		})
	}
}

func TestPingCancellationAndConnectionRelease(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			l, endpoint := pingListener(t, network)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				c, err := l.Accept()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				req, err := wire.ReadRequest(c)
				if err != nil {
					done <- err
					return
				}
				req.Release()
				cancel()
				var b [1]byte
				_, err = c.Read(b[:])
				if err != io.EOF {
					done <- fmt.Errorf("peer read after cancellation: want EOF, got %v", err)
					return
				}
				done <- nil
			}()
			if err := Ping(ctx, endpoint); err == nil {
				t.Fatal("cancelled Ping succeeded")
			}
			l.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Ping(ctx, "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled dial: %v", err)
	}
}

type pingDeadlineConn struct {
	net.Conn
	deadline  time.Time
	t         *testing.T
	exercised *atomic.Bool
}

func (c *pingDeadlineConn) SetDeadline(dl time.Time) error {
	// Cancellation may advance the deadline to now; the first deadline must
	// remain the caller's absolute deadline after time spent dialing.
	if !c.deadline.IsZero() {
		c.exercised.Store(true)
		if dl != c.deadline {
			c.t.Errorf("deadline = %v, want original %v", dl, c.deadline)
		}
		c.deadline = time.Time{}
	}
	return c.Conn.SetDeadline(dl)
}

func TestPingDialAndIOShareBudget(t *testing.T) {
	l, endpoint := pingListener(t, "tcp")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		io.Copy(io.Discard, c) // accepts input but never responds
	}()
	var reachedIO atomic.Bool
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		time.Sleep(50 * time.Millisecond) // emulate a dial consuming part of its budget
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &pingDeadlineConn{Conn: c, deadline: deadline, t: t, exercised: &reachedIO}, nil
	}
	if err := pingEndpoint(ctx, endpoint, dial); err == nil {
		t.Fatal("nonresponsive peer reported healthy")
	}
	l.Close()
	<-done
	if !reachedIO.Load() {
		t.Fatal("dial did not complete; I/O budget was not exercised")
	}
}

type pingBlob struct{ released atomic.Int32 }

func (b *pingBlob) Bytes() []byte     { return []byte("bad payload") }
func (b *pingBlob) Clone() cache.Blob { panic("unexpected Clone") }
func (b *pingBlob) Release()          { b.released.Add(1) }
func TestInvalidPingReleasesBlob(t *testing.T) {
	for _, status := range []byte{wire.StatusHit, wire.StatusError, wire.StatusMiss, wire.StatusCancelled, 255} {
		b := &pingBlob{}
		if err := consumePingResponse(&wire.Response{Status: status, Value: b}); err == nil {
			t.Fatal("unexpected payload accepted")
		}
		if got := b.released.Load(); got != 1 {
			t.Fatalf("Blob released %d times", got)
		}
	}
}

func TestPingMalformedFrame(t *testing.T) {
	for _, kind := range []string{"too-small", "too-large", "error-length", "partial-value"} {
		t.Run(kind, func(t *testing.T) {
			l, endpoint := pingListener(t, "tcp")
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, err := l.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				req, err := wire.ReadRequest(c)
				if err != nil {
					return
				}
				req.Release()
				var frame [wire.ResponseHeaderSize]byte
				binary.LittleEndian.PutUint32(frame[:4], wire.ResponseHeaderSize)
				switch kind {
				case "too-small":
					binary.LittleEndian.PutUint32(frame[:4], 1)
				case "too-large":
					binary.LittleEndian.PutUint32(frame[:4], wire.MaxFrameSize+1)
				case "error-length":
					frame[6] = 1
				case "partial-value":
					binary.LittleEndian.PutUint32(frame[:4], wire.ResponseHeaderSize+10)
				}
				c.Write(frame[:])
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := Ping(ctx, endpoint); err == nil {
				t.Fatal("malformed response accepted")
			}
			l.Close()
			<-done
		})
	}
}
