package prefetch

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
)

// --- AvailabilityIndex integration tests ---

func TestAvailabilityIndex_HasPage_WithBtreeTracker(t *testing.T) {
	// Use a real btree Tracker to parse interior pages.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed an interior page: 1-based [11, 21, 31, 41, 51] → 0-based [10, 20, 30, 40, 50].
	children := []uint32{11, 21, 31, 41, 51}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page) // btree tracker parses children

	ai := NewAvailabilityIndex()

	// Pre-populate peer availability using physical page numbers.
	for _, pg := range []uint32{10, 20, 30, 40, 50} {
		ai.ApplyDelta("peerA", DeltaAdd, pg)
	}

	// All 5 children should be routable to peerA.
	for _, pg := range []uint32{10, 20, 30, 40, 50} {
		if !ai.HasPage("peerA", pg) {
			t.Fatalf("expected HasPage(peerA, %d) = true", pg)
		}
	}

	// Unknown peer should not have pages.
	if ai.HasPage("peerB", 10) {
		t.Fatal("expected HasPage(peerB, 10) = false")
	}
}

func TestAvailabilityIndex_RemovePeer_Integration(t *testing.T) {
	ai := NewAvailabilityIndex()

	for _, pg := range []uint32{10, 20, 30} {
		ai.ApplyDelta("peerA", DeltaAdd, pg)
	}
	for _, pg := range []uint32{20, 30} {
		ai.ApplyDelta("peerB", DeltaAdd, pg)
	}

	ai.RemovePeer("peerA")

	if ai.HasPage("peerA", 10) {
		t.Fatal("expected peerA state removed")
	}
	if !ai.HasPage("peerB", 20) {
		t.Fatal("expected peerB still has page 20")
	}
}

func TestAvailabilityIndex_Reset_Integration(t *testing.T) {
	ai := NewAvailabilityIndex()

	for _, pg := range []uint32{10, 20} {
		ai.ApplyDelta("peerA", DeltaAdd, pg)
	}

	ai.Reset()

	if ai.HasPage("peerA", 10) {
		t.Fatal("expected HasPage=false after reset")
	}
}

// --- Routing simulation (using AvailabilityIndex) ---

type simulatedPeer struct {
	id    string
	pages map[int64]bool
}

type routingScenario struct {
	name           string
	peers          []simulatedPeer
	btreeStructure map[uint32][]uint32 // interior pgno (0-based) → children (0-based)
	accessPattern  []int64
}

type routingMetrics struct {
	peerHits    int
	peerMisses  int
	s3Fallbacks int
	hitRate     float64
}

// runWithAvailability simulates routing using pre-populated AvailabilityIndex
// backed by roaring64 bitmaps over physical page numbers.
func runWithAvailability(sc routingScenario) routingMetrics {
	// Build readahead engine with btree structure (for btree tracking, not availability).
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed interior pages to btree tracker.
	for interiorPgno, children := range sc.btreeStructure {
		oneBased := make([]uint32, len(children))
		for i, c := range children {
			oneBased[i] = c + 1
		}
		page := sqlitebtree.BuildInteriorTablePage(4096, oneBased)
		re.OnFetch(int64(interiorPgno), page)
	}

	ai := NewAvailabilityIndex()

	// Pre-populate each peer's availability using physical page numbers.
	for _, sp := range sc.peers {
		bm := roaring64.New()
		for _, children := range sc.btreeStructure {
			for _, child := range children {
				if sp.pages[int64(child)] {
					bm.Add(uint64(child))
				}
			}
		}
		if bm.GetCardinality() > 0 {
			data, _ := bm.MarshalBinary()
			ai.ApplySnapshot(sp.id, data)
		}
	}

	var m routingMetrics
	for _, pageNo := range sc.accessPattern {
		hit := false
		for _, sp := range sc.peers {
			if ai.HasPage(sp.id, uint32(pageNo)) {
				if sp.pages[pageNo] {
					m.peerHits++
					hit = true
					break
				} else {
					m.peerMisses++
				}
			}
		}
		if !hit {
			m.s3Fallbacks++
		}
	}

	total := m.peerHits + m.peerMisses
	if total > 0 {
		m.hitRate = float64(m.peerHits) / float64(total)
	}
	return m
}

// --- Null implementations for test infrastructure ---

type nullSource struct{}

func (n *nullSource) GetPage(_ context.Context, _ int64) ([]byte, error) {
	return make([]byte, 4096), nil
}

type nullCache struct{}

func (n *nullCache) Get(_ int64) ([]byte, bool)           { return nil, false }
func (n *nullCache) CopyTo(_ int64, _ []byte) (int, bool) { return 0, false }
func (n *nullCache) Put(_ int64, _ []byte)                {}
func (n *nullCache) PutPrefetched(_ int64, _ []byte)      {}
func (n *nullCache) Has(_ int64) bool                     { return false }

// --- Scenario tests ---

func TestRoutingScenario_TableScan(t *testing.T) {
	peerAPages := make(map[int64]bool)
	for i := int64(10); i < 110; i++ {
		peerAPages[i] = true
	}
	peerBPages := make(map[int64]bool)
	for i := int64(200); i < 300; i++ {
		peerBPages[i] = true
	}

	children := make([]uint32, 100)
	for i := range children {
		children[i] = uint32(10 + i)
	}

	pattern := make([]int64, 100)
	for i := range pattern {
		pattern[i] = int64(10 + i)
	}

	sc := routingScenario{
		name:  "table_scan",
		peers: []simulatedPeer{{id: "peerA", pages: peerAPages}, {id: "peerB", pages: peerBPages}},
		btreeStructure: map[uint32][]uint32{
			5: children,
		},
		accessPattern: pattern,
	}

	result := runWithAvailability(sc)

	t.Logf("Availability: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)

	// With pre-populated availability, hit rate should be 100%.
	if result.hitRate < 1.0 {
		t.Fatalf("Availability hit rate = %.2f, want 1.00 (no convergence needed)", result.hitRate)
	}
	if result.peerMisses > 0 {
		t.Fatalf("expected 0 peer misses with exact availability, got %d", result.peerMisses)
	}
}

func TestRoutingScenario_MixedWorkload(t *testing.T) {
	peerAPages := make(map[int64]bool)
	for i := int64(10); i < 60; i++ {
		peerAPages[i] = true
	}
	peerBPages := make(map[int64]bool)
	for i := int64(100); i < 150; i++ {
		peerBPages[i] = true
	}

	childrenA := make([]uint32, 50)
	for i := range childrenA {
		childrenA[i] = uint32(10 + i)
	}
	childrenB := make([]uint32, 50)
	for i := range childrenB {
		childrenB[i] = uint32(100 + i)
	}

	var pattern []int64
	for i := 0; i < 50; i++ {
		pattern = append(pattern, int64(10+i))
		pattern = append(pattern, int64(100+i))
	}

	sc := routingScenario{
		name:  "mixed_workload",
		peers: []simulatedPeer{{id: "peerA", pages: peerAPages}, {id: "peerB", pages: peerBPages}},
		btreeStructure: map[uint32][]uint32{
			5: childrenA,
			6: childrenB,
		},
		accessPattern: pattern,
	}

	result := runWithAvailability(sc)
	t.Logf("Availability: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)

	// 100% hit rate with exact availability — no convergence period.
	if result.hitRate < 1.0 {
		t.Fatalf("Availability hit rate = %.2f, want 1.00", result.hitRate)
	}
}

func TestRoutingScenario_BtreeAmplification(t *testing.T) {
	// With availability, "amplification" is immediate — the peer declares
	// availability for entire interior page child ranges upfront.
	peerAPages := make(map[int64]bool)
	btree := make(map[uint32][]uint32)
	for node := 0; node < 5; node++ {
		children := make([]uint32, 100)
		base := node*100 + 10
		for i := range children {
			pg := uint32(base + i)
			children[i] = pg
			peerAPages[int64(pg)] = true
		}
		btree[uint32(node+5)] = children
	}

	pattern := make([]int64, 500)
	for i := range pattern {
		pattern[i] = int64(i + 10)
	}

	sc := routingScenario{
		name:           "btree_amplification",
		peers:          []simulatedPeer{{id: "peerA", pages: peerAPages}},
		btreeStructure: btree,
		accessPattern:  pattern,
	}

	result := runWithAvailability(sc)
	t.Logf("Availability: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)

	if result.hitRate < 1.0 {
		t.Fatalf("expected 100%% hit rate, got %.2f", result.hitRate)
	}
}

func TestRoutingScenario_PeerDeparture(t *testing.T) {
	ai := NewAvailabilityIndex()

	// Build bitmap for peerA: pages 10..59.
	bmA := roaring64.New()
	for i := uint64(10); i < 60; i++ {
		bmA.Add(i)
	}
	dataA, _ := bmA.MarshalBinary()
	ai.ApplySnapshot("peerA", dataA)

	// Verify peerA pages are routable.
	if !ai.HasPage("peerA", 10) {
		t.Fatal("expected peerA has page 10")
	}

	// Remove peerA.
	ai.RemovePeer("peerA")

	if ai.HasPage("peerA", 10) {
		t.Fatal("expected peerA gone after RemovePeer")
	}

	// Add peerC with same pages.
	ai.ApplySnapshot("peerC", dataA)

	// peerC should immediately be routable.
	for i := uint32(10); i < 60; i++ {
		if !ai.HasPage("peerC", i) {
			t.Fatalf("expected peerC has page %d after snapshot", i)
		}
	}
}

func TestRoutingScenario_LeaderEquivalent(t *testing.T) {
	// With availability gossip, a "leader" just declares it has all pages.
	// No special leader concept needed — just a peer with full availability.
	leaderPages := make(map[int64]bool)
	for i := int64(0); i < 200; i++ {
		leaderPages[i] = true
	}

	children := make([]uint32, 200)
	for i := range children {
		children[i] = uint32(i)
	}

	pattern := make([]int64, 200)
	for i := range pattern {
		pattern[i] = int64(i)
	}

	sc := routingScenario{
		name:  "leader_equivalent",
		peers: []simulatedPeer{{id: "leader", pages: leaderPages}},
		btreeStructure: map[uint32][]uint32{
			5: children,
		},
		accessPattern: pattern,
	}

	result := runWithAvailability(sc)
	t.Logf("Availability: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)

	if result.peerMisses > 0 {
		t.Fatalf("expected 0 peer misses, got %d", result.peerMisses)
	}
	if result.hitRate < 1.0 {
		t.Fatalf("expected 100%% hit rate, got %.2f", result.hitRate)
	}
}
