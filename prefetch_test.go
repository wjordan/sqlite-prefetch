package prefetch

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

type mockSource struct {
	calls   atomic.Int64
	data    map[int64][]byte
	mu      sync.Mutex
	delays  map[int64]chan struct{} // optional: block until released
	entered chan struct{}           // closed on first GetPage call (signals fetch started)
}

func newMockSource(pages map[int64][]byte) *mockSource {
	return &mockSource{data: pages, entered: make(chan struct{})}
}

func (s *mockSource) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	if s.calls.Add(1) == 1 {
		close(s.entered)
	}
	if s.delays != nil {
		s.mu.Lock()
		ch, ok := s.delays[pageNo]
		s.mu.Unlock()
		if ok {
			<-ch
		}
	}
	if d, ok := s.data[pageNo]; ok {
		cp := make([]byte, len(d))
		copy(cp, d)
		return cp, nil
	}
	return make([]byte, 4096), nil
}

type mockCache struct {
	mu    sync.Mutex
	pages map[int64][]byte
}

func newMockCache() *mockCache {
	return &mockCache{pages: make(map[int64][]byte)}
}

func (c *mockCache) Get(pageNo int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.pages[pageNo]
	return d, ok
}

func (c *mockCache) CopyTo(pageNo int64, dst []byte) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.pages[pageNo]
	if !ok {
		return 0, false
	}
	return copy(dst, d), true
}

func (c *mockCache) Put(pageNo int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	c.pages[pageNo] = cp
}

func (c *mockCache) PutPrefetched(pageNo int64, data []byte) {
	c.Put(pageNo, data)
}

func (c *mockCache) Has(pageNo int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pages[pageNo]
	return ok
}

func TestPrefetcher_BasicGetPage(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xAA}, 4096),
		1: bytes.Repeat([]byte{0xBB}, 4096),
	})
	cache := newMockCache()
	p := New(src, cache)

	data, err := p.GetPage(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xAA {
		t.Fatalf("got 0x%02x, want 0xAA", data[0])
	}
	if src.calls.Load() != 1 {
		t.Fatalf("expected 1 source call, got %d", src.calls.Load())
	}
}

func TestPrefetcher_DeduplicatesConcurrent(t *testing.T) {
	// Test the in-flight dedup path deterministically:
	// pre-register an in-flight future, then verify GetPage finds it
	// and returns the future's data without calling the source.
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xFF}, 4096),
	})
	cache := newMockCache()
	p := New(src, cache)

	// Pre-register an in-flight future for page 0.
	f := &pageFuture{done: make(chan struct{})}
	p.mu.Lock()
	p.inflight[0] = f
	p.mu.Unlock()

	// Complete the future with known data (simulates another goroutine's fetch).
	f.data = bytes.Repeat([]byte{0xAA}, 4096)
	close(f.done)

	// GetPage should find the future and return its data without calling source.
	data, err := p.GetPage(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xAA {
		t.Fatalf("got 0x%02x, want 0xAA (should use future data)", data[0])
	}
	if src.calls.Load() != 0 {
		t.Fatalf("expected 0 source calls (used in-flight future), got %d", src.calls.Load())
	}

	// Also verify: a NEW request after the future is completed should go to source
	// (the future was not deleted from inflight, so this tests the path where
	// a second request arrives after the first completed but before cleanup).
	// In real usage, the creator deletes the future, so subsequent requests
	// create new futures.
	p.mu.Lock()
	delete(p.inflight, int64(0)) // simulate creator cleanup
	p.mu.Unlock()

	data2, err := p.GetPage(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if data2[0] != 0xFF {
		t.Fatalf("got 0x%02x, want 0xFF (should use source data)", data2[0])
	}
	if src.calls.Load() != 1 {
		t.Fatalf("expected 1 source call after future cleanup, got %d", src.calls.Load())
	}
}

func TestPrefetcher_ContextCancellation(t *testing.T) {
	delay := make(chan struct{})
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xAA}, 4096),
	})
	src.delays = map[int64]chan struct{}{0: delay}

	cache := newMockCache()
	p := New(src, cache)

	// Start a fetch that will block on the delay.
	go func() {
		p.GetPage(context.Background(), 0)
	}()

	// Wait until the source fetch has started (future is registered).
	<-src.entered

	// Second caller with a cancelled context should return ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.GetPage(ctx, 0)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	close(delay) // clean up
}

func TestPrefetcher_PopulatesCacheAfterFetch(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		3: bytes.Repeat([]byte{0xDD}, 4096),
	})
	cache := newMockCache()
	p := New(src, cache)

	_, err := p.GetPage(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.Get(3); !ok {
		t.Fatal("expected page 3 to be in cache after fetch")
	}
}
