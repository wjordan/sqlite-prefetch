package pagefault

import "sync"

// EWMA tracks a smoothed metric value using exponential weighted moving average.
type EWMA struct {
	mu    sync.Mutex
	value float64
	set   bool
	alpha float64
}

// NewEWMA creates an EWMA with the given smoothing factor (0 < alpha <= 1).
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{alpha: alpha}
}

// Update adds a new sample to the moving average.
func (e *EWMA) Update(sample float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.set {
		e.value = sample
		e.set = true
	} else {
		e.value = e.alpha*sample + (1-e.alpha)*e.value
	}
}

// Value returns the current smoothed value.
func (e *EWMA) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.value
}

// IsSet returns true if at least one sample has been recorded.
func (e *EWMA) IsSet() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.set
}
