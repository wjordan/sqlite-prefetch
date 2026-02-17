package prefetch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SchedulerConfig controls the Scheduler behaviour and defaults.
type SchedulerConfig struct {
	HedgeDelay   time.Duration // 0 = derived from best source latency (1.5x, min 1ms)
	FetchTimeout time.Duration // Default: 5s
	PageSize     int           // Default: 4096
}

func (c *SchedulerConfig) withDefaults() {
	if c.FetchTimeout <= 0 {
		c.FetchTimeout = 5 * time.Second
	}
	if c.PageSize <= 0 {
		c.PageSize = 4096
	}
}

// Scheduler ranks sources by estimated fetch time and uses hedged requests
// to minimize tail latency for single-page faults.
type Scheduler struct {
	mu      sync.RWMutex
	sources []Source
	cfg     SchedulerConfig
}

// NewScheduler creates a Scheduler with the given configuration.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	cfg.withDefaults()
	return &Scheduler{cfg: cfg}
}

// SetSources replaces the set of available sources.
func (s *Scheduler) SetSources(sources []Source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = sources
}

// Sources returns a snapshot of the current source list.
func (s *Scheduler) Sources() []Source {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]Source, len(s.sources))
	copy(cp, s.sources)
	return cp
}

// Fetch retrieves a single page, racing sources with hedged requests.
// The best source (by estimatedTime) is tried immediately. If it hasn't
// responded within the hedge delay, a fallback request is started on the
// next-best source. The first successful response wins.
func (s *Scheduler) Fetch(ctx context.Context, pageNo int64) ([]byte, error) {
	sources := s.sourcesForPage(pageNo)
	if len(sources) == 0 {
		return nil, fmt.Errorf("scheduler: no sources available for page %d", pageNo)
	}
	if len(sources) == 1 {
		return sources[0].GetPage(ctx, pageNo)
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.FetchTimeout)
	defer cancel()

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, len(sources))

	// Start best source immediately.
	go func() {
		data, err := sources[0].GetPage(ctx, pageNo)
		ch <- result{data, err}
	}()

	// Start fallback after hedge delay.
	hedgeDelay := s.hedgeDelay(sources[0])
	go func() {
		select {
		case <-time.After(hedgeDelay):
		case <-ctx.Done():
			ch <- result{nil, ctx.Err()}
			return
		}
		data, err := sources[1].GetPage(ctx, pageNo)
		ch <- result{data, err}
	}()

	var lastErr error
	for i := 0; i < 2; i++ {
		select {
		case r := <-ch:
			if r.err == nil && r.data != nil {
				return r.data, nil
			}
			lastErr = r.err
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// sourcesForPage returns sources that have the page, sorted by estimatedTime
// (ascending). Uses insertion sort since the list is typically 2-3 entries.
func (s *Scheduler) sourcesForPage(pageNo int64) []Source {
	s.mu.RLock()
	all := s.sources
	s.mu.RUnlock()

	candidates := make([]Source, 0, len(all))
	for _, src := range all {
		if src.HasPage(pageNo) {
			candidates = append(candidates, src)
		}
	}
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0; j-- {
			if estimatedTime(candidates[j], s.cfg.PageSize) < estimatedTime(candidates[j-1], s.cfg.PageSize) {
				candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			}
		}
	}
	return candidates
}

// hedgeDelay returns the delay before starting a fallback request.
// If configured explicitly, that value is used. Otherwise it is derived
// as 1.5x the best source's latency, with a minimum of 1ms.
func (s *Scheduler) hedgeDelay(best Source) time.Duration {
	if s.cfg.HedgeDelay > 0 {
		return s.cfg.HedgeDelay
	}
	d := time.Duration(float64(best.Latency()) * 1.5)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}
