package prefetch

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
)

// --- ReadaheadEngine Tests ---

// readaheadTestSource wraps a map and counts fetches.
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

func (c *mockCache) Epoch() uint64 { return 0 }

// cachingObserver wraps FetchObserver and populates the cache on OnFetch,
// simulating the VFS layer's behavior (Fetcher doesn't cache directly).
type cachingObserver struct {
	inner pagefault.FetchObserver
	cache *mockCache
}

func (o *cachingObserver) OnAccess(pageNo int64) { o.inner.OnAccess(pageNo) }
func (o *cachingObserver) OnFetch(pageNo int64, data []byte) {
	o.cache.Put(pageNo, data)
	o.inner.OnFetch(pageNo, data)
}

func TestReadaheadEngine_BtreeWithIndexPages(t *testing.T) {
	src := newReadaheadTestSource(200)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Build interior index page (flag 0x02) with children 100-149.
	indexChildren := make([]uint32, 50)
	for i := range indexChildren {
		indexChildren[i] = uint32(101 + i) // 1-based
	}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, indexChildren)
	interiorPage[0] = 0x02 // interior index
	src.data[5] = interiorPage

	// Fetch interior index page — should be parsed by btree tracker.
	_, err := f.GetPage(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	// Access two consecutive children to trigger scan detection.
	for i := int64(100); i < 103; i++ {
		_, err := f.GetPage(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Btree should have predicted and prefetched remaining siblings.
	prefetched := 0
	for i := int64(103); i < 120; i++ {
		if cache.Has(i) {
			prefetched++
		}
	}
	if prefetched == 0 {
		t.Fatal("expected btree to prefetch index children")
	}
	stats := re.Stats()
	if stats.BtreeHits == 0 {
		t.Fatal("expected btree hits for index page children")
	}
}

func TestReadaheadEngine_RandomNoTrigger(t *testing.T) {
	src := newReadaheadTestSource(200)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Simulate random page faults (not known btree children).
	randoms := []int64{5, 100, 3, 150, 42, 199, 1, 88, 77, 12}
	for _, pg := range randoms {
		_, err := f.GetPage(context.Background(), pg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(20 * time.Millisecond)

	// Should NOT have prefetched much beyond the actual requests.
	cache.mu.Lock()
	totalCached := len(cache.pages)
	cache.mu.Unlock()
	if totalCached > len(randoms)+2 {
		t.Fatalf("expected ~%d cached pages (random, no prefetch), got %d", len(randoms), totalCached)
	}
}

func TestReadaheadEngine_Reset(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})

	// Build up some state.
	re.scanParent = 42
	re.scanIdx = 3
	re.overflowSet[99] = true

	re.Reset()

	if re.scanParent != 0 {
		t.Fatalf("scanParent not reset: %d", re.scanParent)
	}
	if re.scanIdx != -1 {
		t.Fatalf("scanIdx not reset: %d", re.scanIdx)
	}
	if len(re.overflowSet) != 0 {
		t.Fatal("overflowSet not reset")
	}
}

func TestReadaheadEngine_BoundedWorkers(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	f := pagefault.New(src)

	workers := 2
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: workers})

	// Worker semaphore should have capacity = workers.
	if cap(re.workerSem) != workers {
		t.Fatalf("expected sem capacity %d, got %d", workers, cap(re.workerSem))
	}
}

func TestFetcher_GetPageTriggersOnAccess(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	f := pagefault.New(src)

	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 2})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	_, err := f.GetPage(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}

	// Page 5 is not a known btree child, so OnAccess should reset
	// scan state without triggering prefetch.
	stats := re.Stats()
	if stats.BtreeHits != 0 {
		t.Fatal("expected no btree hits for unknown page")
	}
}

func TestFetcher_PrefetchSkipsOnAccess(t *testing.T) {
	src := newReadaheadTestSource(100)
	cache := newMockCache()
	f := pagefault.New(src)

	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 2})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Prefetch should NOT trigger OnAccess.
	_, err := f.Prefetch(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}

	// No OnAccess called → btree hits should be 0, scan state untouched.
	stats := re.Stats()
	if stats.BtreeHits != 0 {
		t.Fatal("expected Prefetch to NOT trigger OnAccess")
	}

	// Verify it was cached.
	if _, ok := cache.Get(5); !ok {
		t.Fatal("expected page 5 to be cached")
	}
}

func TestReadaheadEngine_BtreePrefetchesChildren(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Feed an interior page to the B-tree tracker. Page data contains 1-based
	// SQLite pgno values; OnFetch converts to 0-based: [42, 17, 203, 8, 156].
	btreeChildren := []uint32{43, 18, 204, 9, 157}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access two consecutive children (42 then 17 = children[0] then children[1])
	// to trigger scan detection.
	_, err := f.GetPage(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.GetPage(context.Background(), 17)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify remaining B-tree children were prefetched: [203, 8, 156].
	for _, child := range []int64{203, 8, 156} {
		if !cache.Has(child) {
			t.Errorf("expected B-tree child %d to be prefetched", child)
		}
	}
}

func TestReadaheadEngine_BtreeRandomPattern(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Feed an interior page with scattered children. Page data is 1-based;
	// OnFetch converts to 0-based: [400, 50, 300, 150, 250, 100].
	btreeChildren := []uint32{401, 51, 301, 151, 251, 101}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access two consecutive children (400 then 50 = children[0] then children[1])
	// to trigger scan detection, then continue.
	for _, pg := range []int64{400, 50, 300} {
		_, err := f.GetPage(context.Background(), pg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// B-tree should have prefetched remaining children after 50: [300, 150, 250, 100].
	for _, child := range []int64{150, 250, 100} {
		if !cache.Has(child) {
			t.Errorf("expected B-tree child %d to be prefetched despite random-looking pattern", child)
		}
	}
}

// --- Scan Detection Tests ---

func TestReadaheadEngine_ScanDetectsConsecutiveSiblings(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Interior page with children [10, 20, 30, 40, 50] (0-based).
	btreeChildren := []uint32{11, 21, 31, 41, 51} // 1-based
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access child[0] then child[1] → scan detected, prefetch remaining.
	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.GetPage(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Remaining siblings [30, 40, 50] should be prefetched.
	for _, child := range []int64{30, 40, 50} {
		if !cache.Has(child) {
			t.Errorf("expected sibling %d to be prefetched after scan detection", child)
		}
	}

	stats := re.Stats()
	if stats.BtreeHits == 0 {
		t.Fatal("expected BtreeHits > 0 after scan detection")
	}
}

func TestReadaheadEngine_PointSelectNoPrefetch(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Interior page with children [10, 20, 30, 40, 50] (0-based).
	btreeChildren := []uint32{11, 21, 31, 41, 51}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access only child[0] — point select, no scan detected.
	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Siblings should NOT be prefetched.
	for _, child := range []int64{20, 30, 40, 50} {
		if cache.Has(child) {
			t.Errorf("expected sibling %d NOT to be prefetched on point select", child)
		}
	}

	stats := re.Stats()
	if stats.BtreeHits != 0 {
		t.Fatal("expected no BtreeHits on point select")
	}
}

func TestReadaheadEngine_ScanFromNotifyRead(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Interior page with children [10, 20, 30, 40, 50] (0-based).
	btreeChildren := []uint32{11, 21, 31, 41, 51}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access child[0] via GetPage (cache miss).
	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}

	// Access child[1] via NotifyRead (simulates cache hit path).
	cache.Put(20, make([]byte, 4096))
	f.NotifyRead(20, make([]byte, 4096))

	time.Sleep(50 * time.Millisecond)

	// Scan should be detected: remaining siblings [30, 40, 50] prefetched.
	for _, child := range []int64{30, 40, 50} {
		if !cache.Has(child) {
			t.Errorf("expected sibling %d to be prefetched after NotifyRead scan detection", child)
		}
	}

	stats := re.Stats()
	if stats.BtreeHits == 0 {
		t.Fatal("expected BtreeHits > 0 after scan detection via NotifyRead")
	}
}

func TestReadaheadEngine_NonConsecutiveNoPrefetch(t *testing.T) {
	src := newReadaheadTestSource(500)
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	// Interior page with children [10, 20, 30, 40, 50] (0-based).
	btreeChildren := []uint32{11, 21, 31, 41, 51}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, btreeChildren)
	re.OnFetch(2, interiorPage)

	// Access child[0] then child[3] (skip children[1] and [2]) — not consecutive.
	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.GetPage(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// No scan detected → siblings should NOT be prefetched.
	for _, child := range []int64{20, 30, 50} {
		if cache.Has(child) {
			t.Errorf("expected sibling %d NOT to be prefetched on non-consecutive access", child)
		}
	}

	stats := re.Stats()
	if stats.BtreeHits != 0 {
		t.Fatal("expected no BtreeHits on non-consecutive access")
	}
}

// --- Overflow Chain Tests ---

// buildOverflowPage creates a simulated overflow page with a next-page pointer.
// nextPgno is 1-based (0 means last page in chain).
func buildOverflowPage(pageSize int, nextPgno uint32) []byte {
	page := make([]byte, pageSize)
	binary.BigEndian.PutUint32(page[0:4], nextPgno)
	for i := 4; i < pageSize; i++ {
		page[i] = 0xBB
	}
	return page
}

func TestReadaheadEngine_OverflowFromLeafPage(t *testing.T) {
	cells := []sqlitebtree.LeafCell{
		{PayloadSize: 5000, Rowid: 1, OverflowPgno: 200},
	}
	leafPage := sqlitebtree.BuildLeafTablePage(4096, cells)

	ovfl200 := buildOverflowPage(4096, 201)
	ovfl201 := buildOverflowPage(4096, 202)
	ovfl202 := buildOverflowPage(4096, 0)

	src := &readaheadTestSource{data: map[int64][]byte{
		10:  leafPage,
		199: ovfl200,
		200: ovfl201,
		201: ovfl202,
	}}
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if !cache.Has(199) {
		t.Error("expected first overflow page (0-based 199) to be prefetched")
	}
}

func TestReadaheadEngine_OverflowCascade(t *testing.T) {
	ovfl300 := buildOverflowPage(4096, 301)
	ovfl301 := buildOverflowPage(4096, 302)
	ovfl302 := buildOverflowPage(4096, 0)

	cells := []sqlitebtree.LeafCell{
		{PayloadSize: 5000, Rowid: 1, OverflowPgno: 300},
	}
	leafPage := sqlitebtree.BuildLeafTablePage(4096, cells)

	src := &readaheadTestSource{data: map[int64][]byte{
		10:  leafPage,
		299: ovfl300,
		300: ovfl301,
		301: ovfl302,
	}}
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	_, err := f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	for _, pg := range []int64{299, 300, 301} {
		if !cache.Has(pg) {
			t.Errorf("expected overflow page %d to be prefetched via cascade", pg)
		}
	}

	stats := re.Stats()
	if stats.OverflowHit == 0 {
		t.Error("expected OverflowHit > 0 from cascade")
	}
}

func TestReadaheadEngine_OverflowWithBtreeInteraction(t *testing.T) {
	interiorChildren := []uint32{11, 12}
	interiorPage := sqlitebtree.BuildInteriorTablePage(4096, interiorChildren)

	leaf10Cells := []sqlitebtree.LeafCell{
		{PayloadSize: 5000, Rowid: 1, OverflowPgno: 500},
	}
	leaf10 := sqlitebtree.BuildLeafTablePage(4096, leaf10Cells)

	leaf11Cells := []sqlitebtree.LeafCell{
		{PayloadSize: 5000, Rowid: 2, OverflowPgno: 600},
	}
	leaf11 := sqlitebtree.BuildLeafTablePage(4096, leaf11Cells)

	ovfl500 := buildOverflowPage(4096, 0)
	ovfl600 := buildOverflowPage(4096, 0)

	src := &readaheadTestSource{data: map[int64][]byte{
		2:   interiorPage,
		10:  leaf10,
		11:  leaf11,
		499: ovfl500,
		599: ovfl600,
	}}
	cache := newMockCache()
	f := pagefault.New(src)
	re := NewReadaheadEngine(f, cache, ReadaheadConfig{Workers: 4})
	f.SetObserver(&cachingObserver{inner: re, cache: cache})

	_, err := f.GetPage(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	_, err = f.GetPage(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.GetPage(context.Background(), 11)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if !cache.Has(499) {
		t.Error("expected overflow page 499 to be prefetched from leaf 10")
	}

	stats := re.Stats()
	if stats.BtreeParsed == 0 {
		t.Error("expected btree parsed > 0")
	}
}
