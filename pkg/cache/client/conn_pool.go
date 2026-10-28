package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
)

// ErrCancelled is returned by GetShard/FillShard when the server
// responds with StatusCancelled (in response to a client CancelRequest).
var ErrCancelled = errors.New("client: request cancelled")

// ConnPoolConfig configures a ConnPool.
type ConnPoolConfig struct {
	PoolSize int           // pre-established connections (0 = on-demand only)
	MaxSize  int           // max total connections including burst (must be >= PoolSize)
	Timeout  time.Duration // per-op TCP deadline
	IdleMax  time.Duration // burst connections idle this long get reaped (0 = no reaping)
}

// ConnPool is a channel-based connection pool with elastic burst
// capacity. Pre-established connections are dialled at creation;
// burst connections are dialled on-demand when all pre-established
// conns are busy and current < MaxSize. Excess burst connections
// are reaped after IdleMax of idleness.
type ConnPool struct {
	free    chan *PoolConn
	addr    string
	network string
	config  ConnPoolConfig
	current atomic.Int32 // reserved capacity: free + in-use + dialing
	closed  atomic.Bool
	done    chan struct{}

	// Publication and Close share this lock. The idle Acquire path does not
	// take it, and no network call or retry wait is made while holding it.
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	changed      chan struct{}
	refillNeeded chan struct{}
	maintenance  sync.WaitGroup
}

// PoolConn wraps a single wire.Conn owned by a ConnPool.
type PoolConn struct {
	conn *wire.Conn
	pool *ConnPool
}

// DialConnPool creates a pool with cfg.PoolSize pre-established
// connections. Burst connections up to cfg.MaxSize are dialled
// on-demand by Acquire when all pre-established conns are busy.
func DialConnPool(addr string, cfg ConnPoolConfig) (*ConnPool, error) {
	if cfg.MaxSize < cfg.PoolSize {
		cfg.MaxSize = cfg.PoolSize
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = 1
	}
	// cfg.Timeout <= 0 stays 0 = no per-op deadline (SetOpDeadline
	// clears any deadline rather than setting one when <=0, so ops
	// are bounded only by the caller's context).
	// The TCP/UDS *connect* still keeps a bounded fallback below — a
	// dead listener must not hang the dial forever.
	network, dialAddr := parseAddr(addr)

	p := &ConnPool{
		free:    make(chan *PoolConn, cfg.MaxSize),
		addr:    dialAddr,
		network: network,
		config:  cfg,
		done:    make(chan struct{}),
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.changed = make(chan struct{}, 1)
	p.refillNeeded = make(chan struct{}, 1)
	for i := 0; i < cfg.PoolSize; i++ {
		p.reserve(cfg.MaxSize)
		pc, err := p.dial(p.ctx)
		if err != nil {
			p.releaseCapacity()
			p.Close()
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		p.Release(pc, true)
	}

	if cfg.PoolSize > 0 {
		p.maintenance.Add(1)
		go p.healthLoop()
	}
	if cfg.IdleMax > 0 {
		p.maintenance.Add(1)
		go p.reaperLoop()
	}

	return p, nil
}

const (
	pingInterval = 10 * time.Second
	pingTimeout  = 2 * time.Second
)

// reserve uses the same quota for initial connections, callers and refill.
func (p *ConnPool) reserve(limit int) bool {
	for !p.closed.Load() {
		n := p.current.Load()
		if int(n) >= limit {
			return false
		}
		if p.current.CompareAndSwap(n, n+1) {
			return true
		}
	}
	return false
}

func (p *ConnPool) wake() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (p *ConnPool) releaseCapacity() {
	p.current.Add(-1)
	p.wake()
}

func (p *ConnPool) dial(ctx context.Context) (*PoolConn, error) {
	dialCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()
	defer cancel()
	timeout := p.config.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	raw, err := (&net.Dialer{Timeout: timeout}).DialContext(dialCtx, p.network, p.addr)
	if err != nil {
		return nil, err
	}
	tcpTune(raw)
	return &PoolConn{conn: wire.NewConn(raw), pool: p}, nil
}

func (p *ConnPool) healthLoop() {
	defer p.maintenance.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-p.refillNeeded:
			p.refill()
		case <-ticker.C:
			select {
			case pc := <-p.free:
				p.Release(pc, pc.Ping(pingTimeout) == nil)
			default:
			}
			p.refill()
		}
	}
}

func (p *ConnPool) reaperLoop() {
	defer p.maintenance.Done()
	ticker := time.NewTicker(p.config.IdleMax)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
		for int(p.current.Load()) > p.config.PoolSize {
			select {
			case pc := <-p.free:
				pc.closeConn()
				p.releaseCapacity()
			default:
				goto done
			}
		}
	done:
	}
}

// Acquire returns an idle connection or makes one bounded dialing attempt.
// Only genuine capacity exhaustion waits. Capacity released by a bad
// connection wakes a waiter even when no healthy connection was returned.
func (p *ConnPool) Acquire(ctx context.Context) (*PoolConn, error) {
	for {
		if err := ctx.Err(); err != nil {
			// A cancelled waiter may have consumed the sole capacity wake.
			if int(p.current.Load()) < p.config.MaxSize {
				p.wake()
			}
			return nil, err
		}
		if p.closed.Load() {
			return nil, net.ErrClosed
		}
		select {
		case pc := <-p.free:
			return p.acquired(ctx, pc)
		default:
		}
		if p.reserve(p.config.MaxSize) {
			if int(p.current.Load()) < p.config.MaxSize {
				p.wake()
			}
			pc, err := p.dial(ctx)
			if err != nil {
				p.releaseCapacity()
				return nil, err
			}
			return p.acquired(ctx, pc)
		}
		select {
		case pc := <-p.free:
			return p.acquired(ctx, pc)
		case <-p.changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, net.ErrClosed
		}
	}
}

func (p *ConnPool) acquired(ctx context.Context, pc *PoolConn) (*PoolConn, error) {
	if p.closed.Load() {
		p.Release(pc, false)
		return nil, net.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		p.Release(pc, true)
		return nil, err
	}
	// This waiter may have consumed a capacity notification before taking
	// a returned healthy connection. Relay spare capacity to other waiters.
	if int(p.current.Load()) < p.config.MaxSize {
		p.wake()
	}
	return pc, nil
}

// Release transfers ownership exactly once. A bad connection is discarded;
// one existing maintenance worker may refill, never one goroutine per error.
func (p *ConnPool) Release(pc *PoolConn, healthy bool) {
	p.mu.Lock()
	if healthy && !p.closed.Load() {
		select {
		case p.free <- pc:
			p.mu.Unlock()
			return
		default:
		}
	}
	p.mu.Unlock()
	pc.closeConn()
	p.releaseCapacity()
	if !p.closed.Load() && int(p.current.Load()) < p.config.PoolSize {
		select {
		case p.refillNeeded <- struct{}{}:
		default:
		}
	}
}

// Close prevents publication, wakes all waiters and cancels maintenance
// dials. Borrowed connections remain owned by their callers until Release.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		p.mu.Unlock()
		p.maintenance.Wait()
		return nil
	}
	close(p.done)
	p.cancel()
	var firstErr error
	for {
		select {
		case pc := <-p.free:
			if err := pc.closeConn(); err != nil && firstErr == nil {
				firstErr = err
			}
			p.releaseCapacity()
		default:
			p.mu.Unlock()
			p.maintenance.Wait()
			return firstErr
		}
	}
}

// refill performs single connection attempts under the same reservation
// limit as Acquire. Failure returns to maintenance; read recovery never waits
// for another health tick or for a finite background retry window.
func (p *ConnPool) refill() {
	for p.reserve(p.config.PoolSize) {
		pc, err := p.dial(p.ctx)
		if err != nil {
			p.releaseCapacity()
			return
		}
		p.Release(pc, true)
	}
}

// PoolConn methods

func (pc *PoolConn) WriteRequest(req *wire.Request) error {
	return pc.conn.WriteRequest(req)
}

func (pc *PoolConn) ReadResponse(pool cache.BlobPool) (*wire.Response, error) {
	return pc.conn.ReadResponse(pool)
}

func (pc *PoolConn) SendCancel() error {
	return pc.conn.SendCancel()
}

func (pc *PoolConn) Drain(pool cache.BlobPool) {
	resp, err := pc.conn.ReadResponse(pool)
	if err != nil {
		return
	}
	if resp.Value != nil {
		resp.Value.Release()
	}
}

func (pc *PoolConn) Ping(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return pc.ping(ctx)
}

func (pc *PoolConn) closeConn() error {
	if pc.conn != nil {
		return pc.conn.Close()
	}
	return nil
}

// readOperation makes cancellation interrupt the actual socket operation.
// Its finish function must run before releasing the connection, including
// joining a callback that already started; it never abandons a live read.
func (pc *PoolConn) readOperation(ctx context.Context, timeout time.Duration) func() {
	pc.SetOpDeadline(ctx, timeout)
	if ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = pc.conn.SetDeadline(time.Now())
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

// SetOpDeadline sets read+write deadlines to the earlier of
// (now + timeout) and the ctx deadline. Honouring ctx is essential:
// without it, an upstream hop could keep blocking on a TCP read long
// after the caller's context has expired — and in a TieredCache chain
// that latency propagates to the external client's own timeout, which
// then races the intermediate hop's conn timeout.
//
// Uses a single net.Conn.SetDeadline syscall (covers read + write
// atomically) rather than separate SetReadDeadline + SetWriteDeadline
// (2 syscalls). Per-shard GetShard calls 10 deadline syscalls per Get
// under the old scheme; with this, it's 5.
//
// Pass ctx=nil to use only the timeout budget (legacy behaviour).
func (pc *PoolConn) SetOpDeadline(ctx context.Context, timeout time.Duration) {
	if pc.conn == nil {
		return
	}
	var dl time.Time
	if timeout > 0 {
		dl = time.Now().Add(timeout)
	}
	if ctx != nil {
		if ctxDL, ok := ctx.Deadline(); ok {
			if dl.IsZero() || ctxDL.Before(dl) {
				dl = ctxDL
			}
		}
	}
	if dl.IsZero() {
		// No timeout budget: clear any deadline left over from a prior
		// operation (e.g. a health-check Ping) rather than leaving it
		// in place, since a stale absolute deadline would otherwise
		// expire mid-flight and time out this unrelated operation.
		_ = pc.conn.SetDeadline(time.Time{})
		return
	}
	_ = pc.conn.SetDeadline(dl)
}

// Helpers

func parseAddr(addr string) (network, dialAddr string) {
	if strings.HasPrefix(addr, "unix://") {
		return "unix", strings.TrimPrefix(addr, "unix://")
	}
	if strings.HasPrefix(addr, "/") {
		return "unix", addr
	}
	return "tcp", addr
}

func tcpTune(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
		tc.SetNoDelay(true)
	}
}
