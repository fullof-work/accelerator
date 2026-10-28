// Package appshutdown coordinates the shared drain budget of one App instance.
// It does not own signals, transports, backend resources or process exit.
package appshutdown

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Control separates soft admission shutdown from cancellation of resource users.
// Close ends the graceful budget after users join, before the final resource Close calls.
type Control struct {
	Context     context.Context
	parent      context.Context
	budgetError error
	cancel      context.CancelCauseFunc
	drain       <-chan struct{}
	timeout     time.Duration
	stopping    chan struct{}
	done        chan struct{}
	joined      chan struct{}
	finishOnce  sync.Once
	mu          sync.Mutex
	stopped     bool
	began       time.Time
}

func New(parent context.Context, drain <-chan struct{}, timeout time.Duration) *Control {
	ctx, cancel := context.WithCancelCause(parent)
	c := &Control{Context: ctx, parent: parent, cancel: cancel, drain: drain, timeout: timeout, stopping: make(chan struct{}), done: make(chan struct{}), joined: make(chan struct{})}
	c.budgetError = fmt.Errorf("drain timeout after %s: %w", timeout, context.DeadlineExceeded)
	c.Active(nil)
	go c.watch()
	return c
}

// Active fences startup publication against drain/cancel. fn, when supplied,
// must be short and must not reenter Control. Cancellation takes precedence.
func (c *Control) Active(fn func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		if c.Context.Err() != nil {
			c.stopped = true
		} else {
			select {
			case <-c.drain:
				c.stopped = true
				c.began = time.Now()
			default:
			}
		}
		if c.stopped {
			close(c.stopping)
		}
	}
	if c.stopped {
		return false
	}
	if fn != nil {
		fn()
	}
	return true
}

func (c *Control) Stopping() <-chan struct{} { return c.stopping }

// Cancel closes admission for service failures as well as caller cancellation.
// The App retains the initiating error independently.
func (c *Control) Cancel() { c.cancel(context.Canceled); c.Active(nil) }

func (c *Control) watch() {
	defer close(c.joined)
	select {
	case <-c.Context.Done():
	case <-c.drain:
	case <-c.done:
		return
	}
	c.Active(nil)
	c.mu.Lock()
	began := c.began
	c.mu.Unlock()
	if began.IsZero() || c.timeout == 0 {
		return
	}
	timer := time.NewTimer(time.Until(began.Add(c.timeout)))
	defer timer.Stop()
	select {
	case <-timer.C:
		select {
		case <-c.done:
			return
		default:
		}
		c.cancel(c.budgetError)
	case <-c.Context.Done():
	case <-c.done:
	}
}

// Err preserves deadline/drain-budget failures while retaining the SDK's
// normal context.Canceled shutdown result. Transport and business errors are
// collected by the App independently and must not be discarded.
func (c *Control) Err() error {
	if context.Cause(c.Context) == c.budgetError {
		return c.budgetError
	}
	err := c.parent.Err()
	if err == context.Canceled {
		return nil
	}
	return err
}

func (c *Control) Close() {
	c.finishOnce.Do(func() {
		close(c.done)
		<-c.joined
		c.cancel(context.Canceled)
	})
}
