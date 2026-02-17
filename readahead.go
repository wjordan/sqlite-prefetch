package prefetch

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// PatternType classifies the observed page access pattern.
type PatternType int

const (
	PatternRandom     PatternType = iota
	PatternSequential             // stride == +1
	PatternStride                 // stride == constant non-zero value
)

// patternTracker maintains a ring buffer of recent page numbers and
// classifies the access pattern based on deltas between consecutive entries.
type patternTracker struct {
	buf   [8]int64
	pos   int
	count int
}

// Record adds a page number to the ring buffer.
func (t *patternTracker) Record(pageNo int64) {
	t.buf[t.pos%8] = pageNo
	t.pos++
	if t.count < 8 {
		t.count++
	}
}

// Classify examines the deltas between consecutive entries and returns
// the detected pattern and stride. Needs at least 3 entries (2 deltas).
func (t *patternTracker) Classify() (PatternType, int64) {
	if t.count < 3 {
		return PatternRandom, 0
	}

	// Compute deltas from the ring buffer entries.
	n := t.count
	deltas := make([]int64, 0, n-1)
	for i := 1; i < n; i++ {
		prev := t.buf[(t.pos-n+i-1+8)%8]
		cur := t.buf[(t.pos-n+i+8)%8]
		deltas = append(deltas, cur-prev)
	}

	// Count how many deltas equal +1 (sequential).
	seqCount := 0
	for _, d := range deltas {
		if d == 1 {
			seqCount++
		}
	}
	if seqCount*2 > len(deltas) {
		return PatternSequential, 1
	}

	// Count the most common non-zero delta, grouping ±1 neighbors.
	// LTX file sizes vary slightly, so deltas of 16, 17, 18 should
	// all count as "stride ≈ 17".
	freq := make(map[int64]int, len(deltas))
	for _, d := range deltas {
		if d != 0 {
			freq[d]++
		}
	}
	var bestDelta int64
	var bestCount int
	for d, c := range freq {
		if c > bestCount {
			bestDelta = d
			bestCount = c
		}
	}
	// Also count ±1 neighbors toward the best delta's cluster.
	clusterCount := bestCount + freq[bestDelta-1] + freq[bestDelta+1]
	if clusterCount*2 > len(deltas) {
		return PatternStride, bestDelta
	}

	return PatternRandom, 0
}

// Reset clears the ring buffer.
func (t *patternTracker) Reset() {
	t.pos = 0
	t.count = 0
}

// aimdWindow implements additive-increase multiplicative-decrease window sizing.
type aimdWindow struct {
	size     int
	minSize  int
	maxSize  int
	initSize int
}

func newAIMDWindow(init, min, max int) aimdWindow {
	return aimdWindow{size: init, minSize: min, maxSize: max, initSize: init}
}

// Grow doubles the window size, capped at maxSize.
func (w *aimdWindow) Grow() {
	w.size *= 2
	if w.size > w.maxSize {
		w.size = w.maxSize
	}
}

// Shrink halves the window size, floored at minSize.
func (w *aimdWindow) Shrink() {
	w.size /= 2
	if w.size < w.minSize {
		w.size = w.minSize
	}
}

// Size returns min(size, depth). If depth <= 0, returns size.
func (w *aimdWindow) Size(depth int) int {
	if depth > 0 && depth < w.size {
		return depth
	}
	return w.size
}

// SetMaxSize adjusts the maximum window size (waste feedback).
func (w *aimdWindow) SetMaxSize(max int) {
	if max < w.minSize {
		max = w.minSize
	}
	w.maxSize = max
	if w.size > w.maxSize {
		w.size = w.maxSize
	}
}

// Reset returns the window to initSize.
func (w *aimdWindow) Reset() {
	w.size = w.initSize
}

// interFaultTracker tracks the time between consecutive page faults
// and computes an EWMA of the inter-fault interval.
type interFaultTracker struct {
	lastFault time.Time
	interval  *ewma
}

func newInterFaultTracker() interFaultTracker {
	return interFaultTracker{interval: newEWMA(0.3)}
}

// Record records a fault timestamp and updates the EWMA interval.
func (t *interFaultTracker) Record(now time.Time) {
	if !t.lastFault.IsZero() {
		dt := now.Sub(t.lastFault)
		if dt > 0 {
			t.interval.Update(float64(dt))
		}
	}
	t.lastFault = now
}

// Interval returns the smoothed inter-fault interval.
func (t *interFaultTracker) Interval() time.Duration {
	v := t.interval.Value()
	if v <= 0 {
		return 0
	}
	return time.Duration(v)
}

// Reset clears the tracker state.
func (t *interFaultTracker) Reset() {
	t.lastFault = time.Time{}
	t.interval = newEWMA(0.3)
}

// WasteTracker tracks prefetched pages that were evicted without being accessed.
type WasteTracker struct {
	totalPrefetched atomic.Int64
	wastedEvictions atomic.Int64
}

// NewWasteTracker creates a new WasteTracker.
func NewWasteTracker() *WasteTracker {
	return &WasteTracker{}
}

// OnPrefetched records that a page was prefetched.
func (w *WasteTracker) OnPrefetched() {
	w.totalPrefetched.Add(1)
}

// OnEviction records a cache eviction. If the page was prefetched but never
// accessed, it counts as wasted.
func (w *WasteTracker) OnEviction(prefetched, accessed bool) {
	if prefetched && !accessed {
		w.wastedEvictions.Add(1)
	}
}

// WasteRatio returns the ratio of wasted evictions to total prefetched pages.
// Returns 0 if no pages have been prefetched.
func (w *WasteTracker) WasteRatio() float64 {
	total := w.totalPrefetched.Load()
	if total == 0 {
		return 0
	}
	return float64(w.wastedEvictions.Load()) / float64(total)
}

// ReadaheadConfig controls the ReadaheadEngine parameters.
type ReadaheadConfig struct {
	InitWindow int     // initial AIMD window size (default: 16)
	MinWindow  int     // minimum window size (default: 4)
	MaxWindow  int     // maximum window size (default: 256)
	Workers    int     // max concurrent prefetch goroutines (default: 4)
	WasteHigh  float64 // waste ratio above which maxWindow is halved (default: 0.30)
	WasteLow   float64 // waste ratio below which maxWindow grows toward config max (default: 0.10)
}

func (c *ReadaheadConfig) withDefaults() {
	if c.InitWindow <= 0 {
		c.InitWindow = 16
	}
	if c.MinWindow <= 0 {
		c.MinWindow = 4
	}
	if c.MaxWindow <= 0 {
		c.MaxWindow = 256
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.WasteHigh <= 0 {
		c.WasteHigh = 0.30
	}
	if c.WasteLow <= 0 {
		c.WasteLow = 0.10
	}
}

// ReadaheadEngine predicts upcoming page accesses and prefetches them
// asynchronously. After each synchronous page fault, OnFault is called
// to classify the access pattern and submit background prefetch requests.
type ReadaheadEngine struct {
	mu           sync.Mutex
	pattern      patternTracker
	window       aimdWindow
	faultTracker interFaultTracker
	btree        *btreeTracker
	scheduler    *Scheduler
	prefetcher   *Prefetcher
	cache        PageCache
	wasteTracker *WasteTracker

	// Set-based AIMD feedback: tracks pages from last prefetch batch.
	lastPrefetchedSet  map[uint32]bool
	lastPrefetchActive bool

	workerSem chan struct{} // bounded concurrency
	cfg       ReadaheadConfig
	cfgMax    int // original configured max for waste recovery

	// Diagnostic stats (atomic, lock-free reads).
	statSubmits   atomic.Int64 // total submitBatch calls
	statPages     atomic.Int64 // total pages submitted for prefetch
	statSkipped   atomic.Int64 // pages skipped (already in cache)
	statRandom    atomic.Int64 // OnFault calls classified as Random
	statGrows     atomic.Int64 // AIMD window grow events
	statShrinks   atomic.Int64 // AIMD window shrink events

	statBtreeHits          atomic.Int64 // OnFault calls where B-tree prediction was used
	statBtreeMiss          atomic.Int64 // OnFault calls where B-tree had no prediction
	statBtreeMissNotChild  atomic.Int64 // miss: page not in childToParent
	statBtreeMissLastChild atomic.Int64 // miss: page is the last child
	statBtreeParsed        atomic.Int64 // interior pages successfully parsed by OnFetchComplete
}

// NewReadaheadEngine creates a ReadaheadEngine.
func NewReadaheadEngine(
	prefetcher *Prefetcher,
	scheduler *Scheduler,
	cache PageCache,
	wasteTracker *WasteTracker,
	cfg ReadaheadConfig,
) *ReadaheadEngine {
	cfg.withDefaults()
	return &ReadaheadEngine{
		window:       newAIMDWindow(cfg.InitWindow, cfg.MinWindow, cfg.MaxWindow),
		faultTracker: newInterFaultTracker(),
		btree:        newBtreeTracker(1024),
		scheduler:    scheduler,
		prefetcher:   prefetcher,
		cache:        cache,
		wasteTracker: wasteTracker,
		workerSem:    make(chan struct{}, cfg.Workers),
		cfg:          cfg,
		cfgMax:       cfg.MaxWindow,
	}
}

// OnFault is called after each synchronous page fault. It classifies the
// access pattern and submits background prefetch requests for predicted pages.
func (r *ReadaheadEngine) OnFault(pageNo int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()

	// 1. Record inter-fault timing.
	r.faultTracker.Record(now)

	// 2. AIMD feedback: was this fault predicted by the last prefetch batch?
	if r.lastPrefetchActive {
		if r.inPrefetchedSet(pageNo) {
			r.window.Grow()
			r.statGrows.Add(1)
		} else {
			r.window.Shrink()
			r.statShrinks.Add(1)
		}
	}

	// 3. Record in pattern tracker (keeps P3 warm even when B-tree overrides).
	r.pattern.Record(pageNo)

	// 4. B-tree prediction: try exact child list before P3 stride.
	btPages, result := r.btree.Predict(uint32(pageNo))
	if result == PredictOK {
		depth := r.depthLimit()
		winSize := r.window.Size(depth)
		if winSize > 0 && len(btPages) > winSize {
			btPages = btPages[:winSize]
		}
		r.lastPrefetchedSet = make(map[uint32]bool, len(btPages))
		int64Pages := make([]int64, len(btPages))
		for i, pg := range btPages {
			r.lastPrefetchedSet[pg] = true
			int64Pages[i] = int64(pg)
		}
		r.lastPrefetchActive = true
		r.statBtreeHits.Add(1)
		go r.submitBatch(int64Pages)
		return
	}
	switch result {
	case PredictNotChild:
		r.statBtreeMissNotChild.Add(1)
	case PredictLastChild:
		r.statBtreeMissLastChild.Add(1)
	}
	r.statBtreeMiss.Add(1)

	// 5. Classify access pattern (P3 fallback).
	patType, stride := r.pattern.Classify()
	if patType == PatternRandom {
		r.lastPrefetchActive = false
		r.statRandom.Add(1)
		return
	}

	// 6. Waste feedback: adjust max window based on waste ratio.
	if r.wasteTracker != nil {
		ratio := r.wasteTracker.WasteRatio()
		if ratio > r.cfg.WasteHigh {
			r.window.SetMaxSize(r.window.maxSize / 2)
		} else if ratio < r.cfg.WasteLow && r.window.maxSize < r.cfgMax {
			newMax := r.window.maxSize * 2
			if newMax > r.cfgMax {
				newMax = r.cfgMax
			}
			r.window.SetMaxSize(newMax)
		}
	}

	// 7. Compute depth limit from source latency / inter-fault interval.
	depth := r.depthLimit()

	// 8. Effective window size.
	winSize := r.window.Size(depth)

	// 9. Build page list.
	pages := make([]int64, 0, winSize)
	for i := 1; i <= winSize; i++ {
		pages = append(pages, pageNo+int64(i)*stride)
	}

	// 10. Record prefetch set for next AIMD check.
	r.lastPrefetchedSet = make(map[uint32]bool, len(pages))
	for _, pg := range pages {
		r.lastPrefetchedSet[uint32(pg)] = true
	}
	r.lastPrefetchActive = true

	// 11. Submit batch (non-blocking).
	go r.submitBatch(pages)
}

// OnFetchComplete notifies the B-tree tracker about a fetched page.
// Called from Prefetcher.getPageInternal after every successful fetch,
// and from Prefetcher.NotifyPageRead for cache-hit pages.
func (r *ReadaheadEngine) OnFetchComplete(pageNo int64, data []byte) {
	// Fast-path: skip non-interior pages without acquiring mutex.
	// Interior table pages have flag 0x05. Page 0 (SQLite page 1) has the
	// flag at offset 100 (after the database header).
	if len(data) > 100 {
		flagOff := 0
		if pageNo == 0 {
			flagOff = 100
		}
		if data[flagOff] != 0x05 {
			return
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	prevCount := len(r.btree.interiorChildren)
	r.btree.OnFetchComplete(uint32(pageNo), data)
	if len(r.btree.interiorChildren) > prevCount {
		r.statBtreeParsed.Add(1)
	}
}

// inPrefetchedSet checks if pageNo was in the last prefetch batch.
func (r *ReadaheadEngine) inPrefetchedSet(pageNo int64) bool {
	if !r.lastPrefetchActive {
		return false
	}
	return r.lastPrefetchedSet[uint32(pageNo)]
}

// depthLimit computes how many pages we can usefully prefetch per fault.
// With W parallel workers, we can overlap W fetches during one fault's
// blocking wait, so depth = ceil(W * sourceLatency / interFaultInterval).
func (r *ReadaheadEngine) depthLimit() int {
	ifi := r.faultTracker.Interval()
	if ifi <= 0 {
		return 0 // no limit
	}

	var bestLatency time.Duration
	if r.scheduler != nil {
		sources := r.scheduler.Sources()
		if len(sources) > 0 {
			bestLatency = sources[0].Latency()
		}
	}
	if bestLatency <= 0 {
		return 0 // no limit
	}

	workers := float64(cap(r.workerSem))
	depth := int(math.Ceil(workers * float64(bestLatency) / float64(ifi)))
	if depth < 1 {
		depth = 1
	}
	return depth
}

// submitBatch prefetches pages with bounded concurrency. Skips pages
// already in cache (using Has to avoid polluting hit/miss stats).
func (r *ReadaheadEngine) submitBatch(pages []int64) {
	r.statSubmits.Add(1)
	fetched := int64(0)
	skipped := int64(0)
	for _, pg := range pages {
		if r.cache.Has(pg) {
			skipped++
			continue
		}
		fetched++
		r.workerSem <- struct{}{} // acquire
		go func(pn int64) {
			defer func() { <-r.workerSem }()
			r.prefetcher.getPageInternal(context.Background(), pn, true)
		}(pg)
	}
	r.statPages.Add(fetched)
	r.statSkipped.Add(skipped)
}

// ReadaheadStats contains diagnostic counters for readahead behavior.
type ReadaheadStats struct {
	Submits            int64 // total submitBatch calls
	Pages              int64 // total pages submitted for prefetch
	Skipped            int64 // pages skipped (already in cache)
	Random             int64 // OnFault calls classified as Random
	Grows              int64 // AIMD window grow events
	Shrinks            int64 // AIMD window shrink events
	Window             int   // current AIMD window size
	BtreeHits          int64 // OnFault calls using B-tree prediction
	BtreeMiss          int64 // OnFault calls falling back to P3
	BtreeMissNotChild  int64 // miss: page not in childToParent
	BtreeMissLastChild int64 // miss: page is the last child
	BtreeParsed        int64 // interior pages successfully parsed
}

// Stats returns the current readahead diagnostic counters.
func (r *ReadaheadEngine) Stats() ReadaheadStats {
	r.mu.Lock()
	win := r.window.size
	r.mu.Unlock()
	return ReadaheadStats{
		Submits:            r.statSubmits.Load(),
		Pages:              r.statPages.Load(),
		Skipped:            r.statSkipped.Load(),
		Random:             r.statRandom.Load(),
		Grows:              r.statGrows.Load(),
		Shrinks:            r.statShrinks.Load(),
		Window:             win,
		BtreeHits:          r.statBtreeHits.Load(),
		BtreeMiss:          r.statBtreeMiss.Load(),
		BtreeMissNotChild:  r.statBtreeMissNotChild.Load(),
		BtreeMissLastChild: r.statBtreeMissLastChild.Load(),
		BtreeParsed:        r.statBtreeParsed.Load(),
	}
}

// ResetStats atomically clears all diagnostic counters.
func (r *ReadaheadEngine) ResetStats() {
	r.statSubmits.Store(0)
	r.statPages.Store(0)
	r.statSkipped.Store(0)
	r.statRandom.Store(0)
	r.statGrows.Store(0)
	r.statShrinks.Store(0)
	r.statBtreeHits.Store(0)
	r.statBtreeMiss.Store(0)
	r.statBtreeMissNotChild.Store(0)
	r.statBtreeMissLastChild.Store(0)
	r.statBtreeParsed.Store(0)
}

// Siblings returns all children of pageNo's parent via the btreeTracker.
// Thread-safe: acquires r.mu and returns a copy.
func (r *ReadaheadEngine) Siblings(pageNo uint32) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.btree.Siblings(pageNo)
}

// Reset clears all pattern and window state. Called on rebase when the
// page layout may have changed.
func (r *ReadaheadEngine) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pattern.Reset()
	r.window.Reset()
	r.faultTracker.Reset()
	r.btree.Reset()
	r.lastPrefetchedSet = nil
	r.lastPrefetchActive = false
}
