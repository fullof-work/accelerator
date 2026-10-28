package appshutdown

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDrainDoesNotCancelAndCannotPublish(t *testing.T) {
	drain := make(chan struct{})
	c := New(context.Background(), drain, 0)
	defer c.Close()
	close(drain)
	if c.Active(func() { t.Error("published after drain") }) {
		t.Fatal("drained control is active")
	}
	if c.Context.Err() != nil {
		t.Fatal("soft drain cancelled work")
	}
	select {
	case <-c.Stopping():
	default:
		t.Fatal("admission did not stop")
	}
}

func TestBudgetStartsDuringInitialization(t *testing.T) {
	drain := make(chan struct{})
	c := New(context.Background(), drain, 20*time.Millisecond)
	defer c.Close()
	close(drain)
	// No serving loop has started. This simulates a synchronous provider still
	// in progress; the shared budget must expire without waiting for startup.
	select {
	case <-c.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("initialization hid the drain budget")
	}
	if !errors.Is(c.Err(), context.DeadlineExceeded) {
		t.Fatalf("budget error = %v", c.Err())
	}
	if c.Active(nil) {
		t.Fatal("budget expiration reopened admission")
	}
}

func TestCancellationWinsOverDrain(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		drain := make(chan struct{})
		cancel()
		close(drain)
		c := New(ctx, drain, time.Hour)
		if c.Active(nil) || c.Context.Err() != context.Canceled {
			t.Fatal("cancellation did not win")
		}
		if c.Err() != nil {
			t.Fatalf("SDK cancellation changed: %v", c.Err())
		}
		c.Close()
	}
}

func TestServiceFailureClosesPublicationGate(t *testing.T) {
	c := New(context.Background(), nil, 0)
	defer c.Close()
	c.Cancel()
	if c.Active(func() { t.Error("work added after service failure") }) {
		t.Fatal("gate is open")
	}
}

func TestParentCancellationCausePreservesSDKResult(t *testing.T) {
	cause := errors.New("host reason")
	ctx, cancel := context.WithCancelCause(context.Background())
	c := New(ctx, nil, 0)
	defer c.Close()
	cancel(cause)
	if err := c.Err(); err != nil {
		t.Fatalf("cancel cause changed legacy cancellation result: %v", err)
	}
}

func TestParentDeadlineCausePreservesSDKResult(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, errors.New("host deadline reason"))
	defer cancel()
	c := New(ctx, nil, 0)
	defer c.Close()
	if !errors.Is(c.Err(), context.DeadlineExceeded) {
		t.Fatalf("deadline classification lost: %v", c.Err())
	}
}

func TestBudgetEndsBeforeFinalResourceClose(t *testing.T) {
	drain := make(chan struct{})
	c := New(context.Background(), drain, 20*time.Millisecond)
	close(drain)
	c.Active(nil)
	c.Close() // all users have joined; a dependency's final Close may take longer
	time.Sleep(40 * time.Millisecond)
	if err := c.Err(); err != nil {
		t.Fatalf("final Close consumed grace budget: %v", err)
	}
	c.Close()
}
