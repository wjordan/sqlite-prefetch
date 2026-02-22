package prefetch

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// ReadaheadConfig controls the ReadaheadEngine parameters.
type ReadaheadConfig struct {
	Workers int // max concurrent prefetch goroutines (default: 4)
}

func (c *ReadaheadConfig) withDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
}

// ReadaheadEngine predicts upcoming page accesses and prefetches them
// asynchronously. It detects scans by observing two consecutive sibling
// accesses under the same B-tree parent (analogous to Linux kernel
// ondemand_readahead), then prefetches remaining siblings. Point selects
// that touch only a single child trigger no prefetch.
type ReadaheadEngine struct {
	mu         sync.Mutex
	btree      *btreeTracker
	prefetcher *Prefetcher
	cache      PageCache

	// Scan detection: tracks last accessed page's parent and child index.
	scanParent uint32
	scanIdx    int

	// Overflow chain tracking: known overflow page numbers awaiting fetch.
	overflowSet map[uint32]bool

	workerSem chan struct{} // bounded concurrency

	// Diagnostic stats (atomic, lock-free reads).
	statSubmits     atomic.Int64 // total submitBatch calls
	statPages       atomic.Int64 // total pages submitted for prefetch
	statSkipped     atomic.Int64 // pages skipped (already in cache)
	statBtreeHits   atomic.Int64 // scan detections that triggered B-tree prefetch
	statBtreeParsed atomic.Int64 // interior pages successfully parsed by OnFetchComplete
	statOverflowHit atomic.Int64 // overflow pages prefetched via cascading
}

// NewReadaheadEngine creates a ReadaheadEngine.
func NewReadaheadEngine(
	prefetcher *Prefetcher,
	cache PageCache,
	cfg ReadaheadConfig,
) *ReadaheadEngine {
	cfg.withDefaults()
	return &ReadaheadEngine{
		btree:       newBtreeTracker(1024),
		overflowSet: make(map[uint32]bool),
		prefetcher:  prefetcher,
		cache:       cache,
		workerSem:   make(chan struct{}, cfg.Workers),
		scanIdx:     -1,
	}
}

// OnPageAccess is called on every page read (fault or cache hit) to detect
// scans. Two consecutive sibling accesses under the same B-tree parent
// trigger prefetch of remaining siblings.
func (r *ReadaheadEngine) OnPageAccess(pageNo int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pg := uint32(pageNo)
	parent, ok := r.btree.childToParent[pg]
	if !ok {
		r.scanParent = 0
		r.scanIdx = -1
		return
	}

	children := r.btree.interiorChildren[parent]
	idx := -1
	for i, c := range children {
		if c == pg {
			idx = i
			break
		}
	}
	if idx < 0 {
		r.scanParent = 0
		r.scanIdx = -1
		return
	}

	// Two consecutive siblings under the same parent → scan detected.
	if parent == r.scanParent && idx == r.scanIdx+1 {
		pages, result := r.btree.Predict(pg)
		if result == PredictOK && len(pages) > 0 {
			int64Pages := make([]int64, len(pages))
			for i, p := range pages {
				int64Pages[i] = int64(p)
			}
			r.statBtreeHits.Add(1)
			go r.submitBatch(int64Pages)
		}
	}

	r.scanParent = parent
	r.scanIdx = idx
}

// OnFetchComplete inspects fetched page data for structure that enables
// prefetching. Called from Prefetcher.getPageInternal after every successful
// fetch, and from Prefetcher.NotifyPageRead for cache-hit pages.
//
// Processing order:
//  1. Overflow set check (overflow pages have no flag byte — first 4 bytes
//     are the next-page pointer). If this page is a known overflow page,
//     cascade to the next page in the chain.
//  2. Flag byte check:
//     - 0x05 or 0x02: interior page → parse for btree tracking
//     - 0x0D: leaf table page → extract first overflow page numbers
func (r *ReadaheadEngine) OnFetchComplete(pageNo int64, data []byte) {
	if len(data) < 8 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Check if this is a known overflow page.
	if r.overflowSet[uint32(pageNo)] {
		delete(r.overflowSet, uint32(pageNo))
		r.statOverflowHit.Add(1)

		// Read next-page pointer (first 4 bytes, 1-based SQLite pgno).
		nextPgno := binary.BigEndian.Uint32(data[0:4])
		if nextPgno > 0 {
			next0 := nextPgno - 1 // convert to 0-based
			if len(r.overflowSet) < 4096 {
				r.overflowSet[next0] = true
			}
			go r.submitBatch([]int64{int64(next0)})
		}
		return
	}

	// 2. Check flag byte.
	flagOff := 0
	if pageNo == 0 {
		flagOff = 100
	}
	if flagOff >= len(data) {
		return
	}
	flag := data[flagOff]

	switch flag {
	case 0x05, 0x02: // interior table or index page
		prevCount := len(r.btree.interiorChildren)
		r.btree.OnFetchComplete(uint32(pageNo), data)
		if len(r.btree.interiorChildren) > prevCount {
			r.statBtreeParsed.Add(1)
		}

	case 0x0D: // leaf table page — extract overflow pointers
		sqlitePgno := uint32(pageNo) + 1 // convert to 1-based
		overflows := parseLeafTableOverflows(data, sqlitePgno)
		if len(overflows) > 0 {
			var pages []int64
			for _, ovfl := range overflows {
				ovfl0 := ovfl - 1 // convert to 0-based
				if len(r.overflowSet) < 4096 {
					r.overflowSet[ovfl0] = true
				}
				pages = append(pages, int64(ovfl0))
			}
			go r.submitBatch(pages)
		}
	}
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
	Submits     int64 // total submitBatch calls
	Pages       int64 // total pages submitted for prefetch
	Skipped     int64 // pages skipped (already in cache)
	BtreeHits   int64 // scan detections that triggered B-tree prefetch
	BtreeParsed int64 // interior pages successfully parsed
	OverflowHit int64 // overflow pages prefetched via cascading
}

// Stats returns the current readahead diagnostic counters.
func (r *ReadaheadEngine) Stats() ReadaheadStats {
	return ReadaheadStats{
		Submits:     r.statSubmits.Load(),
		Pages:       r.statPages.Load(),
		Skipped:     r.statSkipped.Load(),
		BtreeHits:   r.statBtreeHits.Load(),
		BtreeParsed: r.statBtreeParsed.Load(),
		OverflowHit: r.statOverflowHit.Load(),
	}
}

// ResetStats atomically clears all diagnostic counters.
func (r *ReadaheadEngine) ResetStats() {
	r.statSubmits.Store(0)
	r.statPages.Store(0)
	r.statSkipped.Store(0)
	r.statBtreeHits.Store(0)
	r.statBtreeParsed.Store(0)
	r.statOverflowHit.Store(0)
}

// Siblings returns all children of pageNo's parent via the btreeTracker.
// Thread-safe: acquires r.mu and returns a copy.
func (r *ReadaheadEngine) Siblings(pageNo uint32) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.btree.Siblings(pageNo)
}

// Reset clears all pattern state. Called on rebase when the page layout
// may have changed.
func (r *ReadaheadEngine) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.btree.Reset()
	r.overflowSet = make(map[uint32]bool)
	r.scanParent = 0
	r.scanIdx = -1
}
