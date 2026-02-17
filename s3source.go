package prefetch

import (
	"context"
	"time"
)

// S3SourceConfig controls the S3Source wrapper behaviour and defaults.
type S3SourceConfig struct {
	EgressCostPerByte float64       // Default: $0.09/GB
	DefaultLatency    time.Duration // Default: 50ms
	DefaultBandwidth  float64       // Default: 100MB/s (bytes/sec)
	EWMAAlpha         float64       // Default: 0.3
}

func (c *S3SourceConfig) withDefaults() {
	if c.EgressCostPerByte <= 0 {
		c.EgressCostPerByte = 0.09 / (1024 * 1024 * 1024)
	}
	if c.DefaultLatency <= 0 {
		c.DefaultLatency = 50 * time.Millisecond
	}
	if c.DefaultBandwidth <= 0 {
		c.DefaultBandwidth = 100 * 1024 * 1024
	}
	if c.EWMAAlpha <= 0 || c.EWMAAlpha > 1 {
		c.EWMAAlpha = 0.3
	}
}

// S3Source wraps a PageSource with Source interface, tracking latency and
// bandwidth via EWMA. S3 is treated as a complete source (Completeness=1.0,
// HasPage always true) since it stores the full database.
type S3Source struct {
	inner     PageSource
	cfg       S3SourceConfig
	latency   *ewma
	bandwidth *ewma
}

// Compile-time check that S3Source satisfies Source.
var _ Source = (*S3Source)(nil)

// NewS3Source creates an S3Source wrapping the given PageSource.
func NewS3Source(inner PageSource, cfg S3SourceConfig) *S3Source {
	cfg.withDefaults()
	lat := newEWMA(cfg.EWMAAlpha)
	lat.Update(float64(cfg.DefaultLatency))
	bw := newEWMA(cfg.EWMAAlpha)
	bw.Update(cfg.DefaultBandwidth)
	return &S3Source{inner: inner, cfg: cfg, latency: lat, bandwidth: bw}
}

func (s *S3Source) Name() string           { return "s3" }
func (s *S3Source) Latency() time.Duration { return time.Duration(s.latency.Value()) }
func (s *S3Source) Bandwidth() float64     { return s.bandwidth.Value() }
func (s *S3Source) Completeness() float64  { return 1.0 }
func (s *S3Source) EgressCost() float64    { return s.cfg.EgressCostPerByte }
func (s *S3Source) HasPage(_ int64) bool   { return true }

// GetPage fetches a page from the inner source, measuring latency and
// bandwidth to update the EWMA trackers.
func (s *S3Source) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	start := time.Now()
	data, err := s.inner.GetPage(ctx, pageNo)
	elapsed := time.Since(start)
	if err == nil && len(data) > 0 {
		s.latency.Update(float64(elapsed))
		if elapsed > 0 {
			s.bandwidth.Update(float64(len(data)) / elapsed.Seconds())
		}
	}
	return data, err
}
