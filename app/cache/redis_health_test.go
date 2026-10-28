package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/health"

	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

type healthStatusRecorder struct {
	mu       sync.Mutex
	statuses []healthgrpc.HealthCheckResponse_ServingStatus
}

type pausedHealthPublication struct {
	server           *health.Server
	entered, release chan struct{}
}

func (p *pausedHealthPublication) SetServingStatus(service string, status healthgrpc.HealthCheckResponse_ServingStatus) {
	close(p.entered)
	<-p.release
	p.server.SetServingStatus(service, status)
}

func TestShutdownFencesAlreadySuccessfulRedisProbe(t *testing.T) {
	h := health.NewServer()
	p := &pausedHealthPublication{server: h, entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := startRedisHealthMonitor(ctx, p, nil)
	<-p.entered // successful probe passed its ctx check but has not published yet
	h.Shutdown()
	cancel()
	close(p.release)
	<-done
	resp, err := h.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil || resp.Status != healthgrpc.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("late health success escaped shutdown: %v, %v", resp, err)
	}
}

func (r *healthStatusRecorder) SetServingStatus(_ string, status healthgrpc.HealthCheckResponse_ServingStatus) {
	r.mu.Lock()
	r.statuses = append(r.statuses, status)
	r.mu.Unlock()
}

func (r *healthStatusRecorder) snapshot() []healthgrpc.HealthCheckResponse_ServingStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]healthgrpc.HealthCheckResponse_ServingStatus(nil), r.statuses...)
}

func TestRedisHealthMonitorCanBeJoinedBeforeFinalStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &healthStatusRecorder{}
	done := startRedisHealthMonitor(ctx, recorder, nil)

	deadline := time.Now().Add(time.Second)
	for len(recorder.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	statuses := recorder.snapshot()
	if len(statuses) == 0 || statuses[len(statuses)-1] != healthgrpc.HealthCheckResponse_SERVING {
		t.Fatalf("initial statuses=%v", statuses)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health monitor did not stop after cancellation")
	}
	recorder.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
	statuses = recorder.snapshot()
	if statuses[len(statuses)-1] != healthgrpc.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("final statuses=%v", statuses)
	}
}
