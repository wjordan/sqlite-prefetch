package pagefault

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
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0x01
			return data, nil
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			time.Sleep(20 * time.Millisecond)
			data := make([]byte, 4096)
			data[0] = 0x02
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
		hasPage:   func(_ int64) bool { return false },
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			t.Fatal("peer.GetPage should not be called")
			return nil, nil
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
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
	_, err := sched.Fetch(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for no sources")
	}

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

func TestScheduler_MultiplePeersMiss_FallsBackToS3(t *testing.T) {
	// Regression test: when 3+ peers claim HasPage but all return miss,
	// the scheduler must still reach S3 (which is sorted last by latency).
	makePeer := func(name string) *staticSource {
		return &staticSource{
			name:      name,
			latency:   5 * time.Millisecond,
			bandwidth: 500 * 1024 * 1024,
			getPage: func(_ context.Context, pageNo int64) ([]byte, error) {
				return nil, fmt.Errorf("%s: miss for page %d", name, pageNo)
			},
		}
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0xAA
			return data, nil
		},
	}

	sched := NewScheduler(SchedulerConfig{HedgeDelay: 1 * time.Millisecond})
	sched.SetSources([]Source{makePeer("peer1"), makePeer("peer2"), makePeer("peer3"), s3})

	data, err := sched.Fetch(context.Background(), 42)
	if err != nil {
		t.Fatalf("expected S3 fallback after all peers miss, got error: %v", err)
	}
	if data[0] != 0xAA {
		t.Fatalf("expected S3 data, got 0x%02X", data[0])
	}
}

func TestScheduler_PeerErrorFallsBackToS3(t *testing.T) {
	peer := &staticSource{
		name:      "peer",
		latency:   2 * time.Millisecond,
		bandwidth: 500 * 1024 * 1024,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			return nil, fmt.Errorf("peer: connection refused")
		},
	}
	s3 := &staticSource{
		name:      "s3",
		latency:   50 * time.Millisecond,
		bandwidth: 100 * 1024 * 1024,
		getPage: func(_ context.Context, _ int64) ([]byte, error) {
			data := make([]byte, 4096)
			data[0] = 0x02
			return data, nil
		},
	}

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
