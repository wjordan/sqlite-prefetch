// Package pagefault provides a generic page-fault handler with in-flight
// deduplication, multi-source scheduling, and hedged requests.
//
// The Fetcher deduplicates concurrent requests for the same page through an
// in-flight map and delegates to a Scheduler (when available) or a direct
// PageSource for the actual fetch. A FetchObserver receives notifications
// about page accesses and completed fetches, allowing callers to implement
// readahead, prefetching, or other reactive behaviors.
//
// The Fetcher does NOT cache results. Caching is the caller's responsibility
// via version-keyed PageCache lookups in the VFS layer.
package pagefault

import (
	"context"
	"sync"
	"sync/atomic"
)

// PageSource fetches page data from storage. Transaction identity is the
// caller's responsibility — use a separate PageSource per read transaction
// when snapshot consistency is required.
type PageSource interface {
	GetPage(ctx context.Context, pageNo int64) ([]byte, error)
}

// FetchObserver receives notifications about page access and fetch events.
type FetchObserver interface {
	// OnAccess is called before a sync page fault (application-initiated GetPage).
	// Not called for Prefetch requests (prevents readahead cascade).
	OnAccess(pageNo int64)

	// OnFetch is called after page data is successfully retrieved, for both
	// GetPage and Prefetch requests.
	OnFetch(pageNo int64, data []byte)
}

// pageFuture represents an in-flight page fetch.
type pageFuture struct {
	done chan struct{}
	data []byte
	err  error
}

// Fetcher coordinates page fetches through an in-flight map to prevent
// duplicate requests. It delegates to a Scheduler (when available) or
// direct PageSource as fallback, and notifies the observer on completion.
type Fetcher struct {
	source    PageSource
	scheduler *Scheduler
	observer  FetchObserver

	mu         sync.Mutex
	inflight   map[int64]*pageFuture
	syncFaults atomic.Int64 // count of GetPage calls (sync faults)
}

// New creates a Fetcher.
func New(source PageSource) *Fetcher {
	return &Fetcher{
		source:   source,
		inflight: make(map[int64]*pageFuture),
	}
}

// SetScheduler sets the fetch scheduler. When set, GetPage delegates to
// the scheduler instead of the direct PageSource.
func (f *Fetcher) SetScheduler(s *Scheduler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduler = s
}

// SetObserver sets the fetch observer for access/fetch notifications.
func (f *Fetcher) SetObserver(obs FetchObserver) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observer = obs
}

// Observer returns the current FetchObserver.
func (f *Fetcher) Observer() FetchObserver {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observer
}

// SyncFaults returns the number of sync page faults (GetPage calls).
func (f *Fetcher) SyncFaults() int64 {
	return f.syncFaults.Load()
}

// ResetSyncFaults atomically reads and resets the sync fault counter.
func (f *Fetcher) ResetSyncFaults() int64 {
	return f.syncFaults.Swap(0)
}

// GetPage returns page data, deduplicating concurrent fetches through
// the in-flight map. Notifies the observer BEFORE the blocking fetch so
// that predicted pages can be fetched concurrently with the current page's
// network round-trip.
func (f *Fetcher) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	f.syncFaults.Add(1)
	f.mu.Lock()
	obs := f.observer
	f.mu.Unlock()
	if obs != nil {
		obs.OnAccess(pageNo)
	}
	return f.fetchInternal(ctx, pageNo, false)
}

// Prefetch fetches a page without triggering OnAccess (prevents readahead
// cascade).
func (f *Fetcher) Prefetch(ctx context.Context, pageNo int64) ([]byte, error) {
	return f.fetchInternal(ctx, pageNo, true)
}

// NotifyRead notifies the observer about a page that was read from cache
// (bypassing the Fetcher fetch path). This ensures pages served from cache
// still get observed for structure parsing and pattern detection.
func (f *Fetcher) NotifyRead(pageNo int64, data []byte) {
	f.mu.Lock()
	obs := f.observer
	f.mu.Unlock()
	if obs != nil {
		obs.OnAccess(pageNo)
		obs.OnFetch(pageNo, data)
	}
}

// fetchInternal fetches a page with in-flight dedup.
func (f *Fetcher) fetchInternal(ctx context.Context, pageNo int64, prefetched bool) ([]byte, error) {
	// 1. Check / create in-flight future.
	f.mu.Lock()
	if fut, ok := f.inflight[pageNo]; ok {
		f.mu.Unlock()
		// Wait for existing fetch to complete.
		select {
		case <-fut.done:
			if fut.err != nil {
				return nil, fut.err
			}
			cp := make([]byte, len(fut.data))
			copy(cp, fut.data)
			return cp, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 2. Create new future.
	fut := &pageFuture{done: make(chan struct{})}
	f.inflight[pageNo] = fut
	f.mu.Unlock()

	// 3. Fetch via scheduler (preferred) or direct source (fallback).
	var data []byte
	var err error
	if f.scheduler != nil {
		data, err = f.scheduler.Fetch(ctx, pageNo)
	} else {
		data, err = f.source.GetPage(ctx, pageNo)
	}

	// 4. Notify observer about fetched page.
	if err == nil && data != nil {
		f.mu.Lock()
		obs := f.observer
		f.mu.Unlock()
		if obs != nil {
			obs.OnFetch(pageNo, data)
		}
	}

	// 5. Signal waiters that data is ready.
	fut.data = data
	fut.err = err
	close(fut.done)

	// 6. Remove from in-flight map.
	f.mu.Lock()
	delete(f.inflight, pageNo)
	f.mu.Unlock()

	return data, err
}
