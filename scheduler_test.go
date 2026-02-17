package prefetch

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestScheduler_SingleSource(t *testing.T) {
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		complete:  1.0,
		getPage: func(_ context.Context, pageNo int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0xAB
			return data, nil
		},
	}

	sched := NewScheduler(SchedulerConfig{})
	sched.SetSources([]Source{s3})

	data, err := sched.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 4096 {
		t.Fatalf("expected 4096 bytes, got %d", len(data))
	}
	if data[0] != 0xAB {
		t.Fatalf("expected first byte 0xAB, got 0x%02X", data[0])
	}
}

func TestScheduler_HedgedRequest_FastWins(t *testing.T) {
	peer := &staticSource{
		name:      "peer",
		latency:   2 * time.Millisecond,
		bandwidth: 500 * 1024 * 1024,
		complete:  0.8,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			// Peer responds quickly.
			data := make([]byte, 4096)
			data[0] = 0x01 // peer marker
			return data, nil
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		complete:  1.0,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			// S3 is much slower.
			time.Sleep(20 * time.Millisecond)
			data := make([]byte, 4096)
			data[0] = 0x02 // s3 marker
			return data, nil
		},
	}

	sched := NewScheduler(SchedulerConfig{HedgeDelay: 5 * time.Millisecond})
	sched.SetSources([]Source{peer, s3})

	data, err := sched.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data[0] != 0x01 {
		t.Fatalf("expected peer (0x01) to win, got 0x%02X", data[0])
	}
}

func TestScheduler_FallbackOnPeerMiss(t *testing.T) {
	peer := &staticSource{
		name:      "peer",
		latency:   2 * time.Millisecond,
		bandwidth: 500 * 1024 * 1024,
		complete:  0.5,
		hasPage:   func(_ int64) bool { return false }, // Peer doesn't have it.
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			t.Fatal("peer.GetPage should not be called")
			return nil, nil
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		complete:  1.0,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0x02
			return data, nil
		},
	}

	sched := NewScheduler(SchedulerConfig{})
	sched.SetSources([]Source{peer, s3})

	data, err := sched.Fetch(context.Background(), 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data[0] != 0x02 {
		t.Fatalf("expected S3 data (0x02), got 0x%02X", data[0])
	}
}

func TestScheduler_NoSources(t *testing.T) {
	sched := NewScheduler(SchedulerConfig{})
	// No sources set at all.
	_, err := sched.Fetch(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for no sources")
	}

	// Sources set but none have the page.
	sched.SetSources([]Source{
		&staticSource{
			name:    "peer",
			hasPage: func(_ int64) bool { return false },
		},
	})
	_, err = sched.Fetch(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error when no source has the page")
	}
}

func TestScheduler_PeerErrorFallsBackToS3(t *testing.T) {
	peer := &staticSource{
		name:      "peer",
		latency:   2 * time.Millisecond,
		bandwidth: 500 * 1024 * 1024,
		complete:  0.8,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			return nil, fmt.Errorf("peer: connection refused")
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		complete:  1.0,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0x02
			return data, nil
		},
	}

	// Short hedge delay so fallback starts quickly after peer error.
	sched := NewScheduler(SchedulerConfig{HedgeDelay: 1 * time.Millisecond})
	sched.SetSources([]Source{peer, s3})

	data, err := sched.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected S3 fallback to succeed, got error: %v", err)
	}
	if data[0] != 0x02 {
		t.Fatalf("expected S3 data (0x02), got 0x%02X", data[0])
	}
}
