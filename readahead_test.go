package prefetch

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// --- Pattern Tracker Tests ---

func TestPatternTracker_Sequential(t *testing.T) {
	var pt patternTracker
	for i := int64(10); i <= 17; i++ {
		pt.Record(i)
	}
	pat, stride := pt.Classify()
	if pat != PatternSequential {
		t.Fatalf("expected Sequential, got %v", pat)
	}
	if stride != 1 {
		t.Fatalf("expected stride 1, got %d", stride)
	}
}

func TestPatternTracker_Stride(t *testing.T) {
	var pt patternTracker
	for i := 0; i < 6; i++ {
		pt.Record(int64(100 + i*3))
	}
	pat, stride := pt.Classify()
	if pat != PatternStride {
		t.Fatalf("expected Stride, got %v", pat)
	}
	if stride != 3 {
		t.Fatalf("expected stride 3, got %d", stride)
	}
}

func TestPatternTracker_NegativeStride(t *testing.T) {
	var pt patternTracker
	for i := 0; i < 5; i++ {
		pt.Record(int64(100 - i*2))
	}
	pat, stride := pt.Classify()
	if pat != PatternStride {
		t.Fatalf("expected Stride, got %v", pat)
	}
	if stride != -2 {
		t.Fatalf("expected stride -2, got %d", stride)
	}
}

func TestPatternTracker_Random(t *testing.T) {
	var pt patternTracker
	pages := []int64{5, 100, 3, 99, 7, 200}
	for _, p := range pages {
		pt.Record(p)
	}
	pat, _ := pt.Classify()
	if pat != PatternRandom {
		t.Fatalf("expected Random, got %v", pat)
	}
}

func TestPatternTracker_InsufficientEntries(t *testing.T) {
	var pt patternTracker
	pt.Record(1)
	pt.Record(2)
	pat, _ := pt.Classify()
	if pat != PatternRandom {
		t.Fatalf("expected Random with <3 entries, got %v", pat)
	}
}

func TestPatternTracker_Reset(t *testing.T) {
	var pt patternTracker
	for i := 0; i < 5; i++ {
		pt.Record(int64(i))
	}
	pt.Reset()
	if pt.count != 0 || pt.pos != 0 {
		t.Fatal("Reset did not clear state")
	}
	pat, _ := pt.Classify()
	if pat != PatternRandom {
		t.Fatalf("expected Random after reset, got %v", pat)
	}
}

func TestPatternTracker_MixedThenSequential(t *testing.T) {
	var pt patternTracker
	// Random entries fill the buffer...
	pt.Record(100)
	pt.Record(5)
	pt.Record(200)
	// ...then sequential entries dominate (ring buffer wraps)
	for i := int64(10); i <= 17; i++ {
		pt.Record(i)
	}
	pat, stride := pt.Classify()
	if pat != PatternSequential {
		t.Fatalf("expected Sequential after wrap, got %v", pat)
	}
	if stride != 1 {
		t.Fatalf("expected stride 1, got %d", stride)
	}
}

// --- AIMD Window Tests ---

func TestAIMDWindow_GrowShrink(t *testing.T) {
	w := newAIMDWindow(16, 4, 256)

	w.Grow()
	if w.size != 32 {
		t.Fatalf("after Grow: expected 32, got %d", w.size)
	}
	w.Shrink()
	if w.size != 16 {
		t.Fatalf("after Shrink: expected 16, got %d", w.size)
	}
}

func TestAIMDWindow_MaxBound(t *testing.T) {
	w := newAIMDWindow(128, 4, 256)
	w.Grow() // 256
	w.Grow() // still 256 (capped)
	if w.size != 256 {
		t.Fatalf("expected 256 (max), got %d", w.size)
	}
}

func TestAIMDWindow_MinBound(t *testing.T) {
	w := newAIMDWindow(8, 4, 256)
	w.Shrink() // 4
	w.Shrink() // still 4 (floored)
	if w.size != 4 {
		t.Fatalf("expected 4 (min), got %d", w.size)
	}
}

func TestAIMDWindow_DepthLimit(t *testing.T) {
	w := newAIMDWindow(64, 4, 256)
	if s := w.Size(10); s != 10 {
		t.Fatalf("expected 10 (depth-limited), got %d", s)
	}
	if s := w.Size(100); s != 64 {
		t.Fatalf("expected 64 (window-limited), got %d", s)
	}
	if s := w.Size(0); s != 64 {
		t.Fatalf("expected 64 (no depth limit), got %d", s)
	}
}

func TestAIMDWindow_SetMaxSize(t *testing.T) {
	w := newAIMDWindow(64, 4, 256)
	w.SetMaxSize(32)
	if w.maxSize != 32 {
		t.Fatalf("expected maxSize 32, got %d", w.maxSize)
	}
	if w.size != 32 {
		t.Fatalf("expected size capped to 32, got %d", w.size)
	}
	// Can't set below min.
	w.SetMaxSize(2)
	if w.maxSize != 4 {
		t.Fatalf("expected maxSize floored to 4, got %d", w.maxSize)
	}
}

func TestAIMDWindow_Reset(t *testing.T) {
	w := newAIMDWindow(16, 4, 256)
	w.Grow()
	w.Grow()
	w.Reset()
	if w.size != 16 {
		t.Fatalf("after Reset: expected 16, got %d", w.size)
	}
}

// --- Inter-Fault Tracker Tests ---

func TestInterFaultTracker_Interval(t *testing.T) {
	ift := newInterFaultTracker()
	base := time.Now()

	ift.Record(base)
	if ift.Interval() != 0 {
		t.Fatal("expected 0 interval after first record")
	}

	ift.Record(base.Add(10 * time.Millisecond))
	ift.Record(base.Add(20 * time.Millisecond))
	ift.Record(base.Add(30 * time.Millisecond))

	interval := ift.Interval()
	if interval < 8*time.Millisecond || interval > 12*time.Millisecond {
		t.Fatalf("expected ~10ms, got %s", interval)
	}
}

func TestInterFaultTracker_Reset(t *testing.T) {
	ift := newInterFaultTracker()
	ift.Record(time.Now())
	ift.Record(time.Now().Add(10 * time.Millisecond))
	ift.Reset()
	if ift.Interval() != 0 {
		t.Fatal("expected 0 after reset")
	}
}

// --- Waste Tracker Tests ---

func TestWasteTracker_Ratio(t *testing.T) {
	wt := NewWasteTracker()
	if r := wt.WasteRatio(); r != 0 {
		t.Fatalf("expected 0 ratio with no data, got %f", r)
	}

	// 10 prefetched, 3 wasted (evicted without access)
	for i := 0; i < 10; i++ {
		wt.OnPrefetched()
	}
	for i := 0; i < 3; i++ {
		wt.OnEviction(true, false) // prefetched, not accessed = waste
	}
	wt.OnEviction(true, true)   // prefetched, accessed = not waste
	wt.OnEviction(false, false) // not prefetched = not waste

	ratio := wt.WasteRatio()
	if ratio < 0.29 || ratio > 0.31 {
		t.Fatalf("expected ~0.3, got %f", ratio)
	}
}

func TestWasteTracker_ZeroPrefetched(t *testing.T) {
	wt := NewWasteTracker()
	wt.OnEviction(true, false) // shouldn't panic
	if r := wt.WasteRatio(); r != 0 {
		t.Fatalf("expected 0 with no prefetched, got %f", r)
	}
}

// --- ReadaheadEngine Tests ---

// countingSource wraps a mockSource and counts getPageInternal calls.
type readaheadTestSource struct {
	data    map[int64][]byte
	fetches atomic.Int64
}

func newReadaheadTestSource(n int) *readaheadTestSource {
	data := make(map[int64][]byte, n)
	for i := 0; i < n; i++ {
		data[int64(i)] = bytes.Repeat([]byte{byte(i % 256)}, 4096)
	}
	return &readaheadTestSource{data: data}
}

func (s *readaheadTestSource) GetPage(_ context.Context, pageNo int64) ([]byte, error) {
	s.fetches.Add(1)
	if d, ok := s.data[pageNo]; ok {
		cp := make([]byte, len(d))
		copy(cp, d)
		return cp, nil
	}
	return make([]byte, 4096), nil
}

func TestReadaheadEngine_SequentialTriggersPrefetch(t *testing.T) {
	src := newReadaheadTestSource(200)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: 4,
	})
	p.SetReadahead(re)

	// Simulate sequential page faults.
	for i := int64(0); i < 10; i++ {
		_, err := p.GetPage(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Give readahead goroutines time to complete.
	time.Sleep(50 * time.Millisecond)

	// Pages beyond 10 should have been prefetched into cache.
	prefetched := 0
	for i := int64(10); i < 30; i++ {
		if _, ok := cache.Get(i); ok {
			prefetched++
		}
	}
	if prefetched == 0 {
		t.Fatal("expected some pages to be prefetched ahead of sequential access")
	}
}

func TestReadaheadEngine_RandomNoTrigger(t *testing.T) {
	src := newReadaheadTestSource(200)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: 4,
	})
	p.SetReadahead(re)

	// Simulate random page faults.
	randoms := []int64{5, 100, 3, 150, 42, 199, 1, 88, 77, 12}
	for _, pg := range randoms {
		_, err := p.GetPage(context.Background(), pg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(20 * time.Millisecond)

	// Should NOT have prefetched much beyond the actual requests.
	totalCached := len(cache.pages)
	if totalCached > len(randoms)+2 {
		t.Fatalf("expected ~%d cached pages (random, no prefetch), got %d", len(randoms), totalCached)
	}
}

func TestReadaheadEngine_StrideTriggersPrefetch(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: 4,
	})
	p.SetReadahead(re)

	// Simulate stride-3 access.
	for i := 0; i < 10; i++ {
		_, err := p.GetPage(context.Background(), int64(i*3))
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Pages at stride-3 beyond 27 should be prefetched.
	prefetched := 0
	for i := 10; i < 20; i++ {
		if _, ok := cache.Get(int64(i * 3)); ok {
			prefetched++
		}
	}
	if prefetched == 0 {
		t.Fatal("expected some pages to be prefetched at stride-3")
	}
}

func TestReadaheadEngine_WindowGrowsOnConfirmation(t *testing.T) {
	re := &ReadaheadEngine{
		window:             newAIMDWindow(8, 2, 64),
		faultTracker:       newInterFaultTracker(),
		wasteTracker:       NewWasteTracker(),
		workerSem:          make(chan struct{}, 4),
		cfg:                ReadaheadConfig{InitWindow: 8, MinWindow: 2, MaxWindow: 64, WasteHigh: 0.3, WasteLow: 0.1},
		cfgMax:             64,
		lastPrefetchedSet:  map[uint32]bool{10: true, 11: true, 12: true, 13: true},
		lastPrefetchActive: true,
	}

	initial := re.window.size

	// Fault on page in the prefetch set → should grow.
	re.mu.Lock()
	if re.inPrefetchedSet(12) {
		re.window.Grow()
	}
	re.mu.Unlock()

	if re.window.size <= initial {
		t.Fatalf("expected window to grow from %d, got %d", initial, re.window.size)
	}
}

func TestReadaheadEngine_WindowShrinksOnMiss(t *testing.T) {
	re := &ReadaheadEngine{
		window:             newAIMDWindow(16, 2, 64),
		faultTracker:       newInterFaultTracker(),
		wasteTracker:       NewWasteTracker(),
		workerSem:          make(chan struct{}, 4),
		cfg:                ReadaheadConfig{InitWindow: 16, MinWindow: 2, MaxWindow: 64, WasteHigh: 0.3, WasteLow: 0.1},
		cfgMax:             64,
		lastPrefetchedSet:  map[uint32]bool{10: true, 11: true, 12: true},
		lastPrefetchActive: true,
	}

	initial := re.window.size

	// Fault on page NOT in the prefetch set → should shrink.
	re.mu.Lock()
	if !re.inPrefetchedSet(100) {
		re.window.Shrink()
	}
	re.mu.Unlock()

	if re.window.size >= initial {
		t.Fatalf("expected window to shrink from %d, got %d", initial, re.window.size)
	}
}

func TestReadaheadEngine_Reset(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 16, MinWindow: 4, MaxWindow: 64, Workers: 4,
	})

	// Build up some state.
	re.pattern.Record(1)
	re.pattern.Record(2)
	re.pattern.Record(3)
	re.lastPrefetchedSet = map[uint32]bool{4: true, 5: true}
	re.lastPrefetchActive = true

	re.Reset()

	if re.pattern.count != 0 {
		t.Fatal("pattern not reset")
	}
	if re.lastPrefetchActive {
		t.Fatal("lastPrefetchActive not reset")
	}
	if re.window.size != 16 {
		t.Fatalf("window not reset to init: %d", re.window.size)
	}
}

func TestReadaheadEngine_BoundedWorkers(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()

	workers := 2
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: workers,
	})

	// Worker semaphore should have capacity = workers.
	if cap(re.workerSem) != workers {
		t.Fatalf("expected sem capacity %d, got %d", workers, cap(re.workerSem))
	}
}

func TestPrefetcher_GetPageTriggersOnFault(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	p := New(src, cache)

	var faults atomic.Int64
	re := &testReadaheadEngine{onFault: func(pageNo int64) { faults.Add(1) }}
	p.readahead = &ReadaheadEngine{} // placeholder; we override OnFault via wrapper

	// Use a real engine but check OnFault is called.
	wt := NewWasteTracker()
	realRe := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 4, MinWindow: 2, MaxWindow: 16, Workers: 2,
	})
	p.SetReadahead(realRe)

	_, err := p.GetPage(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	_ = re // suppress unused

	// OnFault was called inside GetPage (we verify by checking the pattern tracker
	// has recorded the page).
	realRe.mu.Lock()
	count := realRe.pattern.count
	realRe.mu.Unlock()
	if count == 0 {
		t.Fatal("expected OnFault to record the page in pattern tracker")
	}
}

func TestPrefetcher_InternalSkipsOnFault(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	p := New(src, cache)

	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 4, MinWindow: 2, MaxWindow: 16, Workers: 2,
	})
	p.SetReadahead(re)

	// getPageInternal should NOT trigger OnFault.
	_, err := p.getPageInternal(context.Background(), 5, true)
	if err != nil {
		t.Fatal(err)
	}

	re.mu.Lock()
	count := re.pattern.count
	re.mu.Unlock()
	if count != 0 {
		t.Fatal("expected getPageInternal to NOT trigger OnFault")
	}

	// Verify it was cached as prefetched.
	if _, ok := cache.Get(5); !ok {
		t.Fatal("expected page 5 to be cached")
	}
}

// testReadaheadEngine is a stub for testing.
type testReadaheadEngine struct {
	onFault func(pageNo int64)
}

func TestReadaheadEngine_BtreeOverridesStride(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: 4,
	})
	p.SetReadahead(re)

	// Feed an interior page to the B-tree tracker. Page data contains 1-based
	// SQLite pgno values; OnFetchComplete converts to 0-based: [42, 17, 203, 8, 156].
	btreeChildren := []uint32{43, 18, 204, 9, 157}
	interiorPage := buildInteriorTablePage(4096, btreeChildren)
	re.btree.OnFetchComplete(2, interiorPage)

	// Fault on 0-based page 42 → should prefetch [17, 203, 8, 156], NOT stride-based.
	_, err := p.GetPage(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify B-tree children were prefetched.
	for _, child := range []int64{17, 203, 8, 156} {
		if !cache.Has(child) {
			t.Errorf("expected B-tree child %d to be prefetched", child)
		}
	}
}

func TestReadaheadEngine_BtreeFallbackToStride(t *testing.T) {
	src := newReadaheadTestSource(200)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 8, MinWindow: 2, MaxWindow: 32, Workers: 4,
	})
	p.SetReadahead(re)

	// No B-tree data fed — should fall back to P3 stride detection.
	for i := int64(0); i < 10; i++ {
		_, err := p.GetPage(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Sequential pages beyond 10 should have been prefetched (P3 stride).
	prefetched := 0
	for i := int64(10); i < 20; i++ {
		if cache.Has(i) {
			prefetched++
		}
	}
	if prefetched == 0 {
		t.Fatal("expected P3 stride prefetching as fallback")
	}
}

func TestReadaheadEngine_BtreeRandomPattern(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	p := New(src, cache)
	wt := NewWasteTracker()
	re := NewReadaheadEngine(p, nil, cache, wt, ReadaheadConfig{
		InitWindow: 16, MinWindow: 2, MaxWindow: 64, Workers: 4,
	})
	p.SetReadahead(re)

	// Feed an interior page with scattered children. Page data is 1-based;
	// OnFetchComplete converts to 0-based: [400, 50, 300, 150, 250, 100].
	btreeChildren := []uint32{401, 51, 301, 151, 251, 101}
	interiorPage := buildInteriorTablePage(4096, btreeChildren)
	re.btree.OnFetchComplete(2, interiorPage)

	// Access children in order — these look "random" to P3's pattern tracker
	// (deltas: -350, +250, -150, +100, -150) but B-tree knows exactly.
	for _, pg := range []int64{400, 50, 300} {
		_, err := p.GetPage(context.Background(), pg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// B-tree should have prefetched remaining children after 300: [150, 250, 100].
	for _, child := range []int64{150, 250, 100} {
		if !cache.Has(child) {
			t.Errorf("expected B-tree child %d to be prefetched despite random-looking pattern", child)
		}
	}
}
