package pagefault

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

func TestFetcher_BasicGetPage(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xAA}, 4096),
		1: bytes.Repeat([]byte{0xBB}, 4096),
	})
	f := New(src)

	data, err := f.GetPage(context.Background(), 0)
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

func TestFetcher_DeduplicatesConcurrent(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xFF}, 4096),
	})
	f := New(src)

	// Pre-register an in-flight future for page 0.
	fut := &pageFuture{done: make(chan struct{})}
	f.mu.Lock()
	f.inflight[0] = fut
	f.mu.Unlock()

	// Complete the future with known data.
	fut.data = bytes.Repeat([]byte{0xAA}, 4096)
	close(fut.done)

	// GetPage should find the future and return its data without calling source.
	data, err := f.GetPage(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xAA {
		t.Fatalf("got 0x%02x, want 0xAA (should use future data)", data[0])
	}
	if src.calls.Load() != 0 {
		t.Fatalf("expected 0 source calls (used in-flight future), got %d", src.calls.Load())
	}

	// Cleanup and verify new request goes to source.
	f.mu.Lock()
	delete(f.inflight, int64(0))
	f.mu.Unlock()

	data2, err := f.GetPage(context.Background(), 0)
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

func TestFetcher_ContextCancellation(t *testing.T) {
	delay := make(chan struct{})
	src := newMockSource(map[int64][]byte{
		0: bytes.Repeat([]byte{0xAA}, 4096),
	})
	src.delays = map[int64]chan struct{}{0: delay}

	f := New(src)

	// Start a fetch that will block.
	go func() {
		f.GetPage(context.Background(), 0)
	}()
	<-src.entered

	// Second caller with cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.GetPage(ctx, 0)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	close(delay)
}

func TestFetcher_ReturnsDataFromSource(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		3: bytes.Repeat([]byte{0xDD}, 4096),
	})
	f := New(src)

	data, err := f.GetPage(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xDD {
		t.Fatalf("got 0x%02x, want 0xDD", data[0])
	}
}

// mockObserver records observer calls for testing.
type mockObserver struct {
	mu       sync.Mutex
	accesses []int64
	fetches  []int64
}

func (o *mockObserver) OnAccess(pageNo int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.accesses = append(o.accesses, pageNo)
}

func (o *mockObserver) OnFetch(pageNo int64, data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fetches = append(o.fetches, pageNo)
}

func TestFetcher_ObserverNotifications(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		5: bytes.Repeat([]byte{0xAA}, 4096),
	})
	f := New(src)
	obs := &mockObserver{}
	f.SetObserver(obs)

	// GetPage should trigger OnAccess and OnFetch.
	_, err := f.GetPage(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.accesses) != 1 || obs.accesses[0] != 5 {
		t.Fatalf("expected OnAccess(5), got %v", obs.accesses)
	}
	if len(obs.fetches) != 1 || obs.fetches[0] != 5 {
		t.Fatalf("expected OnFetch(5), got %v", obs.fetches)
	}
}

func TestFetcher_PrefetchSkipsOnAccess(t *testing.T) {
	src := newMockSource(map[int64][]byte{
		5: bytes.Repeat([]byte{0xAA}, 4096),
	})
	f := New(src)
	obs := &mockObserver{}
	f.SetObserver(obs)

	// Prefetch should NOT trigger OnAccess, but should trigger OnFetch.
	data, err := f.Prefetch(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0xAA {
		t.Fatalf("got 0x%02x, want 0xAA", data[0])
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.accesses) != 0 {
		t.Fatalf("expected no OnAccess for Prefetch, got %v", obs.accesses)
	}
	if len(obs.fetches) != 1 || obs.fetches[0] != 5 {
		t.Fatalf("expected OnFetch(5), got %v", obs.fetches)
	}
}

func TestFetcher_NotifyRead(t *testing.T) {
	src := newMockSource(nil)
	f := New(src)
	obs := &mockObserver{}
	f.SetObserver(obs)

	f.NotifyRead(42, make([]byte, 4096))

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.accesses) != 1 || obs.accesses[0] != 42 {
		t.Fatalf("expected OnAccess(42), got %v", obs.accesses)
	}
	if len(obs.fetches) != 1 || obs.fetches[0] != 42 {
		t.Fatalf("expected OnFetch(42), got %v", obs.fetches)
	}
}
