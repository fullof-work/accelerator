package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/optrace"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
)

// WireServer accepts TCP/UDS connections and serves cache RPCs using the
// binary wire protocol.
type WireServer struct {
	handler     *CacheHandler
	listener    net.Listener
	wg          sync.WaitGroup
	closing     atomic.Bool
	idleTimeout time.Duration
	rpcTimeout  time.Duration
	tr          *optrace.Tracer

	connsMu sync.Mutex
	conns   map[net.Conn]context.CancelFunc
}

// NewWireServer creates a wire-protocol server.
//
// idleTimeout controls how long an idle connection stays open between
// requests before being closed by the server (0 = no idle timeout).
//
// rpcTimeout is the per-request wall-clock budget passed into
// HandleFrame as a ctx deadline. Backends that honour context
// (tiered-mode remote hops, origin FS reads) will be cancelled when
// the deadline fires; backends that don't (rocks CGO) run to
// completion regardless but the timeout still bounds the wire server
// from issuing a response after the deadline expires. 0 = no per-
// request deadline.
func NewWireServer(handler *CacheHandler, idleTimeout, rpcTimeout time.Duration) *WireServer {
	return &WireServer{
		handler:     handler,
		idleTimeout: idleTimeout,
		rpcTimeout:  rpcTimeout,
		tr:          optrace.FromEnv("cache-ctl"),
		conns:       make(map[net.Conn]context.CancelFunc),
	}
}

// Serve accepts connections on lis until GracefulStop is called.
func (s *WireServer) Serve(lis net.Listener) error {
	s.connsMu.Lock()
	if s.closing.Load() {
		s.connsMu.Unlock()
		_ = lis.Close()
		return nil
	}
	s.listener = lis
	s.connsMu.Unlock()
	defer func() {
		s.connsMu.Lock()
		if s.listener == lis {
			s.listener = nil
		}
		s.connsMu.Unlock()
	}()

	var retryDelay time.Duration
	for {
		conn, err := lis.Accept()
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
				if retryDelay == 0 {
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay *= 2
				}
				if retryDelay > time.Second {
					retryDelay = time.Second
				}
				timer := time.NewTimer(retryDelay)
				<-timer.C
				continue
			}
			return err
		}
		retryDelay = 0
		if !s.startConn(conn) {
			_ = conn.Close()
		}
	}
}

// startConn reserves WaitGroup work before launching the connection goroutine.
// Holding connsMu makes this admission atomic with GracefulStop's transition to
// waiting, so no Add can occur after shutdown has started waiting.
func (s *WireServer) startConn(c net.Conn) bool {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.closing.Load() {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.conns[c] = cancel
	s.wg.Add(1)
	go s.serveConn(ctx, cancel, c)
	return true
}

func (s *WireServer) untrackConn(c net.Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	delete(s.conns, c)
}

// ConnCount returns the number of currently-tracked client connections, for the
// periodic stats line.
func (s *WireServer) ConnCount() int {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return len(s.conns)
}

// Handler exposes the wire handler so the stats printer can read its counters.
func (s *WireServer) Handler() *CacheHandler { return s.handler }

func (s *WireServer) serveConn(connCtx context.Context, cancelConn context.CancelFunc, c net.Conn) {
	defer s.wg.Done()
	defer s.untrackConn(c)
	defer c.Close()
	defer cancelConn()
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
		tc.SetNoDelay(true)
	}
	wc := wire.NewConn(c)
	reqs := make(chan *wire.Request, 2)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(reqs)
		for {
			// Serialize deadline installation with drain: the reader must never
			// overwrite the deadline that interrupts a locally stopped read.
			s.connsMu.Lock()
			if s.closing.Load() || connCtx.Err() != nil {
				s.connsMu.Unlock()
				return
			}
			if s.idleTimeout > 0 {
				_ = wc.SetReadDeadline(time.Now().Add(s.idleTimeout))
			}
			s.connsMu.Unlock()
			req, err := wc.ReadRequest()
			if err != nil {
				// EOF may be a peer's write-half close. Its complete requests still
				// deserve responses. Local drain only stops input, never the handler.
				if !s.closing.Load() && !errors.Is(err, io.EOF) {
					cancelConn()
					_ = c.Close()
				}
				return
			}
			s.connsMu.Lock()
			admitted := !s.closing.Load() && connCtx.Err() == nil
			s.connsMu.Unlock()
			if !admitted {
				req.Release()
				return
			}
			// Admission above is the linearization point. Even if drain now starts,
			// this request belongs to the accepted set, including a blocked send.
			select {
			case reqs <- req:
			case <-connCtx.Done():
				req.Release()
				return
			}
		}
	}()
	var pending []*wire.Request
	drained := false
	defer func() {
		if drained && connCtx.Err() == nil {
			_ = finishResponseDelivery(connCtx, c)
		}
		cancelConn()
		_ = c.Close()
		<-readerDone
		for req := range reqs {
			req.Release()
		}
		for _, req := range pending {
			req.Release()
		}
	}()
	// Every response, including Ping/cancellation, checks transport errors.
	// Its Blob and the request buffer remain live until the write returns.
	write := func(resp *wire.Response) error {
		if resp.Value != nil {
			defer resp.Value.Release()
		}
		return wc.WriteResponse(resp)
	}
	for {
		if connCtx.Err() != nil {
			return
		}
		var req *wire.Request
		if len(pending) > 0 {
			req, pending = pending[0], pending[1:]
		} else {
			var ok bool
			req, ok = <-reqs
			if !ok {
				drained = true
				return
			}
		}
		if req.Opcode == wire.OpcodeCancelRequest {
			req.Release()
			continue
		}
		if req.Opcode == wire.OpcodePing {
			err := write(&wire.Response{Status: wire.StatusHit})
			req.Release()
			if err != nil {
				return
			}
			continue
		}
		var ctx context.Context
		var cancel context.CancelFunc
		if s.rpcTimeout > 0 {
			ctx, cancel = context.WithTimeout(connCtx, s.rpcTimeout)
		} else {
			ctx, cancel = context.WithCancel(connCtx)
		}
		resps := make(chan *wire.Response, 1)
		go func() {
			end := s.tr.Begin("cache.req")
			resp := s.handler.HandleFrame(ctx, req)
			end()
			resps <- resp
		}()
		// A closed input channel is not cancellation. Disable it and continue
		// waiting for the admitted handler, then finish pending and queued work.
		incoming := reqs
		cancelled := false
		failed := false
	inner:
		for {
			select {
			case next, ok := <-incoming:
				if !ok {
					incoming = nil
					continue
				}
				switch next.Opcode {
				case wire.OpcodeCancelRequest:
					next.Release()
					cancel()
					cancelled = true
				case wire.OpcodePing:
					err := write(&wire.Response{Status: wire.StatusHit})
					next.Release()
					if err != nil {
						cancel()
						cancelConn()
						failed = true
					}
				default:
					pending = append(pending, next)
				}
			case resp := <-resps:
				cancel()
				if connCtx.Err() != nil || failed {
					if resp.Value != nil {
						resp.Value.Release()
					}
					req.Release()
					return
				}
				if cancelled {
					if resp.Value != nil {
						resp.Value.Release()
					}
					resp = &wire.Response{Status: wire.StatusCancelled}
				}
				err := write(resp)
				req.Release()
				if err != nil {
					return
				}
				if cancelled {
					for len(pending) > 0 {
						pr := pending[0]
						pending = pending[1:]
						err = write(&wire.Response{Status: wire.StatusCancelled})
						pr.Release()
						if err != nil {
							return
						}
					}
				}
				break inner
			}
		}
	}
}

// Drain atomically stops connection and request admission. It interrupts input
// reads without cancelling accepted handlers or closing their write halves.
// Wait joins all handlers, response writes and readers. Stop can escalate it.
func (s *WireServer) Drain() { s.stop(false) }

func (s *WireServer) stop(force bool) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	s.closing.Store(true)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for c, cancel := range s.conns {
		if force {
			cancel()
			_ = c.Close()
		} else {
			_ = c.SetReadDeadline(time.Now())
		}
	}
}

// GracefulStop drains every admitted request through its complete response.
// There is no internal grace deadline; callers may concurrently escalate Stop.
func (s *WireServer) GracefulStop() { s.Drain(); s.Wait() }

// Stop cancels work and closes transports, then waits for actual resource users.
// An uncancellable backend may delay return; its dependencies must remain live.
func (s *WireServer) Stop() { s.stop(true); s.Wait() }

// Wait joins accepted connection goroutines. Call Drain or Stop first to fence
// new WaitGroup additions. It includes readers, handlers and response writers.
func (s *WireServer) Wait() { s.wg.Wait() }
