package prefetch

import (
	"context"
	"sync"
	"sync/atomic"
)

// PageSource fetches page data from storage.
type PageSource interface {
	GetPage(ctx context.Context, pageNo int64) ([]byte, error)
}

// PageCache reads and writes cached pages.
type PageCache interface {
	Get(pageNo int64) ([]byte, bool)
	CopyTo(pageNo int64, dst []byte) (int, bool)
	Put(pageNo int64, data []byte)
	PutPrefetched(pageNo int64, data []byte)
	Has(pageNo int64) bool // non-stat-tracking existence check
}

// pageFuture represents an in-flight page fetch.
type pageFuture struct {
	done chan struct{}
	data []byte
	err  error
}

// Prefetcher coordinates page fetches through an in-flight map to prevent
// duplicate requests. It checks the cache first, then deduplicates fetches
// to the underlying source (via Scheduler when available, or direct PageSource
// as fallback).
type Prefetcher struct {
	source     PageSource
	scheduler  *Scheduler
	cache      PageCache
	readahead  *ReadaheadEngine
	peerRouter *PeerRouter

	mu         sync.Mutex
	inflight   map[int64]*pageFuture
	syncFaults atomic.Int64 // count of GetPage calls (sync faults from SQLite)
}

// New creates a Prefetcher.
func New(source PageSource, cache PageCache) *Prefetcher {
	return &Prefetcher{
		source:   source,
		cache:    cache,
		inflight: make(map[int64]*pageFuture),
	}
}

// SetScheduler sets the fetch scheduler. When set, GetPage delegates to
// the scheduler instead of the direct PageSource.
func (p *Prefetcher) SetScheduler(s *Scheduler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheduler = s
}

// SetReadahead sets the readahead engine for async prefetching.
func (p *Prefetcher) SetReadahead(r *ReadaheadEngine) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readahead = r
}

// SetPeerRouter sets the peer router for reactive peer routing.
func (p *Prefetcher) SetPeerRouter(r *PeerRouter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peerRouter = r
}

// Readahead returns the current ReadaheadEngine (for test toggling).
func (p *Prefetcher) Readahead() *ReadaheadEngine {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readahead
}

// ResetReadahead clears the readahead engine's pattern state and peer router
// page hints. Called on rebase when the page layout may have changed.
func (p *Prefetcher) ResetReadahead() {
	p.mu.Lock()
	r := p.readahead
	pr := p.peerRouter
	p.mu.Unlock()
	if r != nil {
		r.Reset()
	}
	if pr != nil {
		pr.Reset()
	}
}

// SyncFaults returns the number of sync page faults (GetPage calls from SQLite).
func (p *Prefetcher) SyncFaults() int64 {
	return p.syncFaults.Load()
}

// ResetSyncFaults atomically reads and resets the sync fault counter.
func (p *Prefetcher) ResetSyncFaults() int64 {
	return p.syncFaults.Swap(0)
}

// GetPage returns page data, deduplicating concurrent fetches through
// the in-flight map. Notifies the readahead engine BEFORE the blocking
// fetch so that predicted pages can be fetched concurrently with the
// current page's S3 round-trip.
func (p *Prefetcher) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	p.syncFaults.Add(1)
	p.mu.Lock()
	r := p.readahead
	p.mu.Unlock()
	if r != nil {
		r.OnPageAccess(pageNo)
	}
	return p.getPageInternal(ctx, pageNo, false)
}

// getPageInternal fetches a page with in-flight dedup. When prefetched is true,
// uses PutPrefetched for waste tracking and does NOT trigger readahead (prevents
// cascade). The submitBatch caller already checks cache.Has() before spawning,
// so no redundant cache check is needed here.
func (p *Prefetcher) getPageInternal(ctx context.Context, pageNo int64, prefetched bool) ([]byte, error) {
	// 1. Check / create in-flight future.
	p.mu.Lock()
	if f, ok := p.inflight[pageNo]; ok {
		p.mu.Unlock()
		// Wait for existing fetch to complete.
		select {
		case <-f.done:
			if f.err != nil {
				return nil, f.err
			}
			cp := make([]byte, len(f.data))
			copy(cp, f.data)
			return cp, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 2. Create new future.
	f := &pageFuture{done: make(chan struct{})}
	p.inflight[pageNo] = f
	p.mu.Unlock()

	// 3. Fetch via scheduler (preferred) or direct source (fallback).
	var data []byte
	var err error
	if p.scheduler != nil {
		data, err = p.scheduler.Fetch(ctx, pageNo)
	} else {
		data, err = p.source.GetPage(ctx, pageNo)
	}
	// 4. Populate cache on success.
	if err == nil && data != nil {
		if prefetched {
			p.cache.PutPrefetched(pageNo, data)
		} else {
			p.cache.Put(pageNo, data)
		}
	}

	// 5. Notify readahead B-tree tracker about fetched page.
	if err == nil && data != nil {
		p.mu.Lock()
		r := p.readahead
		p.mu.Unlock()
		if r != nil {
			r.OnFetchComplete(pageNo, data)
		}
	}

	// 6. Signal waiters that data is ready.
	f.data = data
	f.err = err
	close(f.done)

	// 7. Remove from in-flight map.
	p.mu.Lock()
	delete(p.inflight, pageNo)
	p.mu.Unlock()

	return data, err
}

// NotifyPageRead notifies the readahead B-tree tracker about a page that
// was read from cache (bypassing the Prefetcher fetch path). This ensures
// interior B-tree pages served from cache still get parsed, so their children
// are registered in the B-tree tracker before SQLite descends into them.
func (p *Prefetcher) NotifyPageRead(pageNo int64, data []byte) {
	p.mu.Lock()
	r := p.readahead
	p.mu.Unlock()
	if r != nil {
		r.OnPageAccess(pageNo)
		r.OnFetchComplete(pageNo, data)
	}
}
