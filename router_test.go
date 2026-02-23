package prefetch

import (
	"context"
	"math/rand"
	"testing"

	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
)

// --- AvailabilityIndex integration tests ---

func TestAvailabilityIndex_HasPage_WithBtreeTracker(t *testing.T) {
	// Use a real btree Tracker as the ChildLookup.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed an interior page: 1-based [11, 21, 31, 41, 51] → 0-based [10, 20, 30, 40, 50].
	children := []uint32{11, 21, 31, 41, 51}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page) // 0-based page 2

	ai := NewAvailabilityIndex(re.Btree())

	// Pre-populate peer availability for all children of interior page 2.
	ai.OnInteriorPageParsed(2)
	ai.ApplyDelta("peerA", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 2,
		Extents:      []ChildExtent{{Start: 0, Count: 5}},
	})

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
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	children := []uint32{11, 21, 31}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page)

	ai := NewAvailabilityIndex(re.Btree())
	ai.OnInteriorPageParsed(2)

	ai.ApplyDelta("peerA", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 2,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})
	ai.ApplyDelta("peerB", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 2,
		Extents:      []ChildExtent{{Start: 1, Count: 2}},
	})

	ai.RemovePeer("peerA")

	if ai.HasPage("peerA", 10) {
		t.Fatal("expected peerA state removed")
	}
	if !ai.HasPage("peerB", 20) {
		t.Fatal("expected peerB still has page 20")
	}
}

func TestAvailabilityIndex_Reset_Integration(t *testing.T) {
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	children := []uint32{11, 21}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page)

	ai := NewAvailabilityIndex(re.Btree())
	ai.OnInteriorPageParsed(2)
	ai.ApplyDelta("peerA", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 2,
		Extents:      []ChildExtent{{Start: 0, Count: 2}},
	})

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

// runWithAvailability simulates routing using pre-populated AvailabilityIndex.
// Since availability is known upfront (from gossip), there is no convergence
// period — every query is either a hit or correctly falls to S3.
func runWithAvailability(sc routingScenario) routingMetrics {
	// Build readahead engine with btree structure.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed interior pages.
	for interiorPgno, children := range sc.btreeStructure {
		oneBased := make([]uint32, len(children))
		for i, c := range children {
			oneBased[i] = c + 1
		}
		page := sqlitebtree.BuildInteriorTablePage(4096, oneBased)
		re.OnFetch(int64(interiorPgno), page)
	}

	ai := NewAvailabilityIndex(re.Btree())

	// Mark all interior pages as parsed.
	for interiorPgno := range sc.btreeStructure {
		ai.OnInteriorPageParsed(interiorPgno)
	}

	// Pre-populate each peer's availability from their page set.
	for _, sp := range sc.peers {
		var pages []PageAvailability
		for interiorPgno, children := range sc.btreeStructure {
			var cached []uint16
			for i, child := range children {
				if sp.pages[int64(child)] {
					cached = append(cached, uint16(i))
				}
			}
			if len(cached) == 0 {
				continue
			}
			// Build extents from cached indices.
			// Sort (already in order since we iterate i=0..n).
			var extents []ChildExtent
			start := cached[0]
			count := uint16(1)
			for j := 1; j < len(cached); j++ {
				if cached[j] == start+count {
					count++
				} else {
					extents = append(extents, ChildExtent{Start: start, Count: count})
					start = cached[j]
					count = 1
				}
			}
			extents = append(extents, ChildExtent{Start: start, Count: count})
			pages = append(pages, PageAvailability{InteriorPage: interiorPgno, Extents: extents})
		}
		if len(pages) > 0 {
			ai.ApplySnapshot(sp.id, pages)
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

func runWithBloomFilter(sc routingScenario) routingMetrics {
	type peerBloom struct {
		id     string
		pages  map[int64]bool
		filter *bloomSim
	}
	var peers []peerBloom
	for _, sp := range sc.peers {
		bf := newBloomSim(len(sp.pages), 0.01)
		for pg := range sp.pages {
			bf.add(pg)
		}
		peers = append(peers, peerBloom{id: sp.id, pages: sp.pages, filter: bf})
	}

	var m routingMetrics
	rng := rand.New(rand.NewSource(42))

	for _, pageNo := range sc.accessPattern {
		var candidates []peerBloom
		for _, pb := range peers {
			if pb.filter.test(pageNo) {
				candidates = append(candidates, pb)
			}
		}

		if len(candidates) == 0 {
			m.s3Fallbacks++
		} else {
			chosen := candidates[rng.Intn(len(candidates))]
			if chosen.pages[pageNo] {
				m.peerHits++
			} else {
				m.peerMisses++
			}
		}
	}

	total := m.peerHits + m.peerMisses
	if total > 0 {
		m.hitRate = float64(m.peerHits) / float64(total)
	}
	return m
}

// bloomSim is a simple simulation bloom filter for comparison testing.
type bloomSim struct {
	bits []bool
	k    int
}

func newBloomSim(n int, fpRate float64) *bloomSim {
	if n < 1 {
		n = 1
	}
	m := int(-float64(n) * 4.0 / 0.48)
	if m < 64 {
		m = 64
	}
	k := int(float64(m) / float64(n) * 0.693)
	if k < 1 {
		k = 1
	}
	return &bloomSim{bits: make([]bool, m), k: k}
}

func (b *bloomSim) add(pageNo int64) {
	for _, pos := range b.hashes(pageNo) {
		b.bits[pos] = true
	}
}

func (b *bloomSim) test(pageNo int64) bool {
	for _, pos := range b.hashes(pageNo) {
		if !b.bits[pos] {
			return false
		}
	}
	return true
}

func (b *bloomSim) hashes(pageNo int64) []int {
	h1 := uint64(pageNo)*2654435761 + 1
	h2 := uint64(pageNo)*40503 + 17
	result := make([]int, b.k)
	for i := 0; i < b.k; i++ {
		result[i] = int((h1 + uint64(i)*h2) % uint64(len(b.bits)))
	}
	return result
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
	bloomResult := runWithBloomFilter(sc)

	t.Logf("Availability: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)
	t.Logf("BloomFilter: hitRate=%.2f hits=%d misses=%d s3=%d",
		bloomResult.hitRate, bloomResult.peerHits, bloomResult.peerMisses, bloomResult.s3Fallbacks)

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
	peerAPages := make(map[int64]bool)
	for i := int64(10); i < 60; i++ {
		peerAPages[i] = true
	}

	children := make([]uint32, 50)
	for i := range children {
		children[i] = uint32(10 + i)
	}

	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})
	oneBased := make([]uint32, len(children))
	for i, c := range children {
		oneBased[i] = c + 1
	}
	page := sqlitebtree.BuildInteriorTablePage(4096, oneBased)
	re.OnFetch(5, page)

	ai := NewAvailabilityIndex(re.Btree())
	ai.OnInteriorPageParsed(5)

	// Build extents for peerA.
	ai.ApplySnapshot("peerA", []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 50}}},
	})

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
	ai.ApplySnapshot("peerC", []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 50}}},
	})

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
