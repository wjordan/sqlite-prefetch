package prefetch

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestS3Source_Properties(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		42: bytes.Repeat([]byte{0xAA}, 4096),
	})
	s := NewS3Source(src, S3SourceConfig{})

	if s.Name() != "s3" {
		t.Fatalf("Name() = %q, want %q", s.Name(), "s3")
	}
	if s.Completeness() != 1.0 {
		t.Fatalf("Completeness() = %f, want 1.0", s.Completeness())
	}
	if !s.HasPage(42) {
		t.Fatal("HasPage(42) = false, want true")
	}
	if !s.HasPage(999) {
		t.Fatal("HasPage(999) = false, want true (S3 has all pages)")
	}
	if s.EgressCost() <= 0 {
		t.Fatalf("EgressCost() = %e, want > 0", s.EgressCost())
	}
	// Default latency should be ~50ms.
	if s.Latency() < 40*time.Millisecond || s.Latency() > 60*time.Millisecond {
		t.Fatalf("Latency() = %v, want ~50ms", s.Latency())
	}
	// Default bandwidth should be ~100MB/s.
	bw := s.Bandwidth()
	if bw < 90*1024*1024 || bw > 110*1024*1024 {
		t.Fatalf("Bandwidth() = %f, want ~100MB/s", bw)
	}
}

func TestS3Source_GetPage(t *testing.T) {
	pages := map[int64][]byte{
		1: bytes.Repeat([]byte{0xBB}, 4096),
		5: bytes.Repeat([]byte{0xCC}, 4096),
	}
	src := newMockSource(pages)
	s := NewS3Source(src, S3SourceConfig{})

	data, err := s.GetPage(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4096 {
		t.Fatalf("len(data) = %d, want 4096", len(data))
	}
	if data[0] != 0xBB {
		t.Fatalf("data[0] = 0x%02x, want 0xBB", data[0])
	}

	data, err = s.GetPage(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xCC {
		t.Fatalf("data[0] = 0x%02x, want 0xCC", data[0])
	}

	if src.calls.Load() != 2 {
		t.Fatalf("inner source calls = %d, want 2", src.calls.Load())
	}
}

func TestS3Source_MeasuresLatency(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		1: bytes.Repeat([]byte{0xAA}, 4096),
	})
	s := NewS3Source(src, S3SourceConfig{EWMAAlpha: 0.5})

	initialLatency := s.Latency()

	// Perform several fast fetches (in-memory mock is near-instant).
	for i := 0; i < 10; i++ {
		_, err := s.GetPage(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
	}

	afterLatency := s.Latency()
	if afterLatency >= initialLatency {
		t.Fatalf("latency should decrease after fast fetches: initial=%v after=%v", initialLatency, afterLatency)
	}
	// The mock source is nearly instant, so latency should converge well below the 50ms default.
	if afterLatency > 10*time.Millisecond {
		t.Fatalf("latency should converge near zero for instant source, got %v", afterLatency)
	}
}

func TestS3Source_DefaultConfig(t *testing.T) {
	cfg := S3SourceConfig{}
	cfg.withDefaults()

	// EgressCostPerByte should be $0.09/GB.
	expectedCost := 0.09 / (1024 * 1024 * 1024)
	if cfg.EgressCostPerByte != expectedCost {
		t.Fatalf("EgressCostPerByte = %e, want %e", cfg.EgressCostPerByte, expectedCost)
	}
	if cfg.DefaultLatency != 50*time.Millisecond {
		t.Fatalf("DefaultLatency = %v, want 50ms", cfg.DefaultLatency)
	}
	if cfg.DefaultBandwidth != 100*1024*1024 {
		t.Fatalf("DefaultBandwidth = %f, want %f", cfg.DefaultBandwidth, float64(100*1024*1024))
	}
	if cfg.EWMAAlpha != 0.3 {
		t.Fatalf("EWMAAlpha = %f, want 0.3", cfg.EWMAAlpha)
	}
}

func TestS3Source_CustomConfig(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		1: bytes.Repeat([]byte{0xAA}, 4096),
	})
	cfg := S3SourceConfig{
		EgressCostPerByte: 0.05 / (1024 * 1024 * 1024),
		DefaultLatency:    100 * time.Millisecond,
		DefaultBandwidth:  200 * 1024 * 1024,
		EWMAAlpha:         0.5,
	}
	s := NewS3Source(src, cfg)

	if s.EgressCost() != cfg.EgressCostPerByte {
		t.Fatalf("EgressCost() = %e, want %e", s.EgressCost(), cfg.EgressCostPerByte)
	}
	// Initial latency should reflect the custom default.
	if s.Latency() < 90*time.Millisecond || s.Latency() > 110*time.Millisecond {
		t.Fatalf("Latency() = %v, want ~100ms", s.Latency())
	}
}

func TestS3Source_ImplementsSource(t *testing.T) {
	src := newMockSource(nil)
	s := NewS3Source(src, S3SourceConfig{})
	// Verify the interface is satisfied at the type level.
	var _ Source = s
}
