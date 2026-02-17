package prefetch

import (
	"context"
	"math"
	"sync"
	"time"
)

// Source is a page data provider with measurable performance properties.
type Source interface {
	Name() string
	Latency() time.Duration
	Bandwidth() float64
	Completeness() float64
	EgressCost() float64
	HasPage(pageNo int64) bool
	GetPage(ctx context.Context, pageNo int64) ([]byte, error)
}

// ewma tracks a smoothed metric value using exponential weighted moving average.
type ewma struct {
	mu    sync.Mutex
	value float64
	set   bool
	alpha float64
}

func newEWMA(alpha float64) *ewma {
	return &ewma{alpha: alpha}
}

func (e *ewma) Update(sample float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.set {
		e.value = sample
		e.set = true
	} else {
		e.value = e.alpha*sample + (1-e.alpha)*e.value
	}
}

func (e *ewma) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.value
}

func (e *ewma) IsSet() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.set
}

// estimatedTime computes the expected time to fetch `bytes` from a source.
func estimatedTime(s Source, bytes int) time.Duration {
	bw := s.Bandwidth()
	if bw <= 0 {
		return s.Latency() + time.Duration(math.MaxInt64/2)
	}
	transferTime := time.Duration(float64(bytes) / bw * float64(time.Second))
	return s.Latency() + transferTime
}
