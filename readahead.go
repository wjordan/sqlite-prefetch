package prefetch

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
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
//
// ReadaheadEngine implements pagefault.FetchObserver.
type ReadaheadEngine struct {
	mu      sync.Mutex
	btree   *sqlitebtree.Tracker
	fetcher *pagefault.Fetcher
	cache   pagefault.PageCache

	// Availability tracking.
	localAvail  *LocalAvailability
	remoteAvail *AvailabilityIndex

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
	statBtreeParsed atomic.Int64 // interior pages successfully parsed by OnFetch
	statOverflowHit atomic.Int64 // overflow pages prefetched via cascading
}

// Compile-time check that ReadaheadEngine satisfies pagefault.FetchObserver.
var _ pagefault.FetchObserver = (*ReadaheadEngine)(nil)

// NewReadaheadEngine creates a ReadaheadEngine. The localAvail and
// remoteAvail parameters are optional; pass nil to disable availability
// tracking.
func NewReadaheadEngine(
	fetcher *pagefault.Fetcher,
	cache pagefault.PageCache,
	cfg ReadaheadConfig,
) *ReadaheadEngine {
	cfg.withDefaults()
	return &ReadaheadEngine{
		btree:       sqlitebtree.NewTracker(1024),
		overflowSet: make(map[uint32]bool),
		fetcher:     fetcher,
		cache:       cache,
		workerSem:   make(chan struct{}, cfg.Workers),
		scanIdx:     -1,
	}
}

// SetAvailability sets the local and remote availability trackers. Both are
// optional (nil-safe).
func (r *ReadaheadEngine) SetAvailability(local *LocalAvailability, remote *AvailabilityIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.localAvail = local
	r.remoteAvail = remote
}

// OnAccess is called on every page read (fault or cache hit) to detect
// scans. Two consecutive sibling accesses under the same B-tree parent
// trigger prefetch of remaining siblings.
//
// Implements pagefault.FetchObserver.
func (r *ReadaheadEngine) OnAccess(pageNo int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pg := uint32(pageNo)
	parent, idx, ok := r.btree.ChildPosition(pg)
	if !ok {
		r.scanParent = 0
		r.scanIdx = -1
		return
	}

	// Two consecutive siblings under the same parent → scan detected.
	if parent == r.scanParent && idx == r.scanIdx+1 {
		pages, result := r.btree.Predict(pg)
		if result == sqlitebtree.PredictOK && len(pages) > 0 {
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

// OnFetch inspects fetched page data for structure that enables prefetching.
// Called after every successful fetch.
//
// Processing order:
//  1. Overflow set check (overflow pages have no flag byte — first 4 bytes
//     are the next-page pointer). If this page is a known overflow page,
//     cascade to the next page in the chain.
//  2. Flag byte check:
//     - 0x05 or 0x02: interior page → parse for btree tracking
//     - 0x0D: leaf table page → extract first overflow page numbers
//
// Implements pagefault.FetchObserver.
func (r *ReadaheadEngine) OnFetch(pageNo int64, data []byte) {
	if len(data) < 8 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Notify local availability that this page is now cached.
	if r.localAvail != nil {
		r.localAvail.OnPageCached(uint32(pageNo))
	}

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
		r.btree.OnFetchComplete(uint32(pageNo), data)
		r.statBtreeParsed.Add(1)

		// Notify availability trackers about the parsed interior page.
		if children, ok := r.btree.Children(uint32(pageNo)); ok {
			if r.localAvail != nil {
				r.localAvail.OnInteriorPageParsed(uint32(pageNo), children)
			}
			if r.remoteAvail != nil {
				r.remoteAvail.OnInteriorPageParsed(uint32(pageNo))
			}
		}

	case 0x0D: // leaf table page — extract overflow pointers
		sqlitePgno := uint32(pageNo) + 1 // convert to 1-based
		overflows := sqlitebtree.ParseLeafTableOverflows(data, sqlitePgno)
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
			r.fetcher.Prefetch(context.Background(), pn)
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

// Btree returns the underlying B-tree tracker. Used by AvailabilityIndex
// as a ChildLookup.
func (r *ReadaheadEngine) Btree() *sqlitebtree.Tracker {
	return r.btree
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
