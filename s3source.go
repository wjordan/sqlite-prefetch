package prefetch

import (
	"context"
	"time"

	"github.com/wjordan/sqlite-prefetch/pagefault"
)

// S3SourceConfig controls the S3Source wrapper behaviour and defaults.
type S3SourceConfig struct {
	DefaultLatency   time.Duration // Default: 50ms
	DefaultBandwidth float64       // Default: 100MB/s (bytes/sec)
	EWMAAlpha        float64       // Default: 0.3
}

func (c *S3SourceConfig) withDefaults() {
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
// bandwidth via EWMA. S3 always has every page (HasPage always true).
type S3Source struct {
	inner     pagefault.PageSource
	cfg       S3SourceConfig
	latency   *pagefault.EWMA
	bandwidth *pagefault.EWMA
}

// Compile-time check that S3Source satisfies pagefault.Source.
var _ pagefault.Source = (*S3Source)(nil)

// NewS3Source creates an S3Source wrapping the given PageSource.
func NewS3Source(inner pagefault.PageSource, cfg S3SourceConfig) *S3Source {
	cfg.withDefaults()
	lat := pagefault.NewEWMA(cfg.EWMAAlpha)
	lat.Update(float64(cfg.DefaultLatency))
	bw := pagefault.NewEWMA(cfg.EWMAAlpha)
	bw.Update(cfg.DefaultBandwidth)
	return &S3Source{inner: inner, cfg: cfg, latency: lat, bandwidth: bw}
}

func (s *S3Source) Name() string           { return "s3" }
func (s *S3Source) Latency() time.Duration { return time.Duration(s.latency.Value()) }
func (s *S3Source) Bandwidth() float64     { return s.bandwidth.Value() }
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
