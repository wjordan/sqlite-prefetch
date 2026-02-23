package pagefault

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestEWMA_FirstSample(t *testing.T) {
	e := NewEWMA(0.3)
	if e.IsSet() {
		t.Fatal("expected IsSet()=false before any update")
	}
	e.Update(100)
	if !e.IsSet() {
		t.Fatal("expected IsSet()=true after update")
	}
	if got := e.Value(); got != 100 {
		t.Fatalf("expected first sample to set value directly: got %f, want 100", got)
	}
}

func TestEWMA_Smoothing(t *testing.T) {
	e := NewEWMA(0.3)
	e.Update(100)
	e.Update(200)
	// Expected: 0.3*200 + 0.7*100 = 60 + 70 = 130
	want := 130.0
	if got := e.Value(); math.Abs(got-want) > 0.001 {
		t.Fatalf("expected smoothed value %f, got %f", want, got)
	}
}

// staticSource implements Source with fixed values for testing.
type staticSource struct {
	name      string
	latency   time.Duration
	bandwidth float64
	hasPage   func(int64) bool
	getPage   func(context.Context, int64) ([]byte, error)
}

func (s *staticSource) Name() string           { return s.name }
func (s *staticSource) Latency() time.Duration { return s.latency }
func (s *staticSource) Bandwidth() float64     { return s.bandwidth }

func (s *staticSource) HasPage(pageNo int64) bool {
	if s.hasPage != nil {
		return s.hasPage(pageNo)
	}
	return true
}

func (s *staticSource) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	if s.getPage != nil {
		return s.getPage(ctx, pageNo)
	}
	return make([]byte, 4096), nil
}

func TestEstimatedTime(t *testing.T) {
	src := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 1e9, // 1 GB/s
	}
	got := EstimatedTime(src, 4096)
	// Transfer time: 4096 / 1e9 seconds = ~4.096us
	// Total: 50ms + ~4us ≈ 50.004ms
	if got < 50*time.Millisecond || got > 51*time.Millisecond {
		t.Fatalf("expected ~50ms, got %v", got)
	}
}

func TestEstimatedTime_ZeroBandwidth(t *testing.T) {
	src := &staticSource{
		name:      "stalled",
		latency:   10 * time.Millisecond,
		bandwidth: 0,
	}
	got := EstimatedTime(src, 4096)
	if got < time.Hour {
		t.Fatalf("expected very large duration for zero bandwidth, got %v", got)
	}
}
