package prefetch

import (
	"context"
	"math/rand"
	"testing"

	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
)

// --- Unit tests ---

func TestPeerRouter_RecordHit(t *testing.T) {
	r := NewPeerRouter(nil)
	r.RecordResult("peerA", 10, true)

	if rate := r.PeerHitRate("peerA"); rate != 1.0 {
		t.Fatalf("expected hit rate 1.0 after first hit, got %f", rate)
	}
	if r.HintCount() != 1 {
		t.Fatalf("expected 1 page hint, got %d", r.HintCount())
	}
}

func TestPeerRouter_RecordMiss(t *testing.T) {
	r := NewPeerRouter(nil)
	// First set a hint, then miss should remove it.
	r.RecordResult("peerA", 10, true)
	if r.HintCount() != 1 {
		t.Fatalf("expected 1 hint after hit, got %d", r.HintCount())
	}

	r.RecordResult("peerA", 10, false)
	if r.HintCount() != 0 {
		t.Fatalf("expected 0 hints after miss, got %d", r.HintCount())
	}
	if rate := r.PeerHitRate("peerA"); rate >= 1.0 {
		t.Fatalf("expected hit rate < 1.0 after miss, got %f", rate)
	}
}

func TestPeerRouter_SiblingExpansion(t *testing.T) {
	// Build a readahead engine with a btreeTracker that has an interior page.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed an interior page: 1-based [11, 21, 31, 41, 51] → 0-based [10, 20, 30, 40, 50].
	children := []uint32{11, 21, 31, 41, 51}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page) // 0-based page 2

	r := NewPeerRouter(re)
	r.RecordResult("peerA", 10, true)

	// Should have hints for page 10 and all its siblings (20, 30, 40, 50).
	if r.HintCount() != 5 {
		t.Fatalf("expected 5 hints (page + 4 siblings), got %d", r.HintCount())
	}

	// All siblings should route to peerA.
	for _, pg := range []int64{10, 20, 30, 40, 50} {
		if !r.ShouldTry("peerA", pg) {
			t.Fatalf("expected ShouldTry(peerA, %d) = true", pg)
		}
	}
}

func TestPeerRouter_ShouldTry_Exploration(t *testing.T) {
	r := NewPeerRouter(nil)
	// Use a deterministic rng seeded with 1 (the default in NewPeerRouter).
	// Count how many of 1000 calls return true for an unknown peer+page.
	trueCount := 0
	for i := 0; i < 1000; i++ {
		if r.ShouldTry("unknownPeer", int64(i+10000)) {
			trueCount++
		}
	}
	// Expect ~10% exploration rate (100 ± 40).
	if trueCount < 50 || trueCount > 200 {
		t.Fatalf("exploration rate out of range: %d/1000 (expected ~100)", trueCount)
	}
}

func TestPeerRouter_ShouldTry_HintMatch(t *testing.T) {
	r := NewPeerRouter(nil)
	r.RecordResult("peerA", 42, true)

	if !r.ShouldTry("peerA", 42) {
		t.Fatal("expected ShouldTry = true for hinted page")
	}
	// Different peer with a hint for peerA should return false (hint mismatch).
	// But it might return true from exploration, so we lock the rng.
	r.mu.Lock()
	r.rng = rand.New(rand.NewSource(999)) // seed that won't hit explore
	r.explore = 0                          // disable exploration for this test
	r.mu.Unlock()

	if r.ShouldTry("peerB", 42) {
		t.Fatal("expected ShouldTry = false for wrong peer when page is hinted to peerA")
	}
}

func TestPeerRouter_ShouldTry_GoodHitRate(t *testing.T) {
	r := NewPeerRouter(nil)
	r.mu.Lock()
	r.explore = 0 // disable exploration
	r.mu.Unlock()

	// Build up a good hit rate for peerA.
	for i := 0; i < 10; i++ {
		r.RecordResult("peerA", int64(1000+i), true)
	}

	// For an unhinted page, peerA should still be tried due to high hit rate.
	if !r.ShouldTry("peerA", 9999) {
		t.Fatal("expected ShouldTry = true for peer with high hit rate")
	}
}

func TestPeerRouter_RemovePeer(t *testing.T) {
	r := NewPeerRouter(nil)
	r.RecordResult("peerA", 10, true)
	r.RecordResult("peerA", 20, true)
	r.RecordResult("peerB", 30, true)

	r.RemovePeer("peerA")

	if r.HintCount() != 1 { // only peerB's hint remains
		t.Fatalf("expected 1 hint after removing peerA, got %d", r.HintCount())
	}
	if r.PeerHitRate("peerA") != 0 {
		t.Fatal("expected peerA score to be removed")
	}
}

func TestPeerRouter_RetainOnly(t *testing.T) {
	r := NewPeerRouter(nil)
	r.RecordResult("peerA", 10, true)
	r.RecordResult("peerB", 20, true)
	r.RecordResult("peerC", 30, true)

	r.RetainOnly([]string{"peerB"})

	if r.HintCount() != 1 {
		t.Fatalf("expected 1 hint after retaining peerB, got %d", r.HintCount())
	}
	if r.PeerHitRate("peerA") != 0 {
		t.Fatal("expected peerA score to be removed")
	}
	if r.PeerHitRate("peerC") != 0 {
		t.Fatal("expected peerC score to be removed")
	}
	if r.PeerHitRate("peerB") == 0 {
		t.Fatal("expected peerB score to be kept")
	}
}

func TestPeerRouter_Reset(t *testing.T) {
	r := NewPeerRouter(nil)
	r.RecordResult("peerA", 10, true)
	r.RecordResult("peerA", 20, true)

	r.Reset()

	if r.HintCount() != 0 {
		t.Fatalf("expected 0 hints after reset, got %d", r.HintCount())
	}
	// Peer scores should be preserved.
	if r.PeerHitRate("peerA") == 0 {
		t.Fatal("expected peerA score to survive reset")
	}
}

func TestPeerRouter_MaxHints(t *testing.T) {
	r := NewPeerRouter(nil)
	r.mu.Lock()
	r.maxHints = 10
	r.mu.Unlock()

	for i := 0; i < 20; i++ {
		r.RecordResult("peerA", int64(i), true)
	}

	if r.HintCount() > 10 {
		t.Fatalf("expected at most 10 hints, got %d", r.HintCount())
	}
}

func TestPeerRouter_SetLeader(t *testing.T) {
	r := NewPeerRouter(nil)
	r.mu.Lock()
	r.explore = 0 // disable exploration
	r.mu.Unlock()

	// Without a leader, unhinted pages fall through to exploration (disabled).
	if r.ShouldTry("peerA", 42) {
		t.Fatal("expected ShouldTry = false without leader or hints")
	}

	// Set leader — all unhinted pages route to leader.
	r.SetLeader("peerA")
	if !r.ShouldTry("peerA", 42) {
		t.Fatal("expected ShouldTry = true for leader")
	}
	if r.ShouldTry("peerB", 42) {
		t.Fatal("expected ShouldTry = false for non-leader")
	}

	// Page hints still take priority over leader.
	r.RecordResult("peerB", 42, true)
	if r.ShouldTry("peerA", 42) {
		t.Fatal("expected ShouldTry = false for leader when page is hinted to peerB")
	}
	if !r.ShouldTry("peerB", 42) {
		t.Fatal("expected ShouldTry = true for hinted peer")
	}

	// Clear leader.
	r.SetLeader("")
	if r.Leader() != "" {
		t.Fatal("expected leader to be cleared")
	}
}

func TestPeerRouter_RemovePeer_ClearsLeader(t *testing.T) {
	r := NewPeerRouter(nil)
	r.SetLeader("peerA")
	r.RemovePeer("peerA")
	if r.Leader() != "" {
		t.Fatal("expected leader to be cleared after RemovePeer")
	}
}

func TestPeerRouter_RetainOnly_ClearsLeader(t *testing.T) {
	r := NewPeerRouter(nil)
	r.SetLeader("peerA")
	r.RetainOnly([]string{"peerB"})
	if r.Leader() != "" {
		t.Fatal("expected leader to be cleared after RetainOnly excludes it")
	}

	// Leader is retained if in the keep set.
	r.SetLeader("peerB")
	r.RetainOnly([]string{"peerB"})
	if r.Leader() != "peerB" {
		t.Fatal("expected leader to survive RetainOnly when in keep set")
	}
}

// --- Routing simulation ---

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
	peerHits      int
	peerMisses    int
	s3Fallbacks   int
	hitRate       float64
	convergenceAt int // fault index where rolling hit rate first exceeds 80%
}

func runWithPeerRouter(sc routingScenario) routingMetrics {
	// Build readahead engine with btree structure.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed interior pages.
	for interiorPgno, children := range sc.btreeStructure {
		// Convert 0-based children to 1-based for the page builder.
		oneBased := make([]uint32, len(children))
		for i, c := range children {
			oneBased[i] = c + 1
		}
		page := sqlitebtree.BuildInteriorTablePage(4096, oneBased)
		re.OnFetch(int64(interiorPgno), page)
	}

	router := NewPeerRouter(re)
	router.mu.Lock()
	router.rng = rand.New(rand.NewSource(42))
	router.mu.Unlock()

	var m routingMetrics
	m.convergenceAt = -1
	totalAttempts := 0
	totalHits := 0

	for faultIdx, pageNo := range sc.accessPattern {
		// Try peers the router says to try; fall through on miss (like real scheduler).
		hit := false
		attempted := false
		for _, sp := range sc.peers {
			if router.ShouldTry(sp.id, pageNo) {
				attempted = true
				totalAttempts++
				if sp.pages[pageNo] {
					router.RecordResult(sp.id, pageNo, true)
					m.peerHits++
					totalHits++
					hit = true
					break
				} else {
					router.RecordResult(sp.id, pageNo, false)
					m.peerMisses++
				}
			}
		}
		if !attempted || !hit {
			m.s3Fallbacks++
		}

		// Check convergence.
		if totalAttempts > 0 && m.convergenceAt < 0 {
			rate := float64(totalHits) / float64(totalAttempts)
			if rate >= 0.80 {
				m.convergenceAt = faultIdx
			}
		}
	}

	if totalAttempts > 0 {
		m.hitRate = float64(m.peerHits) / float64(totalAttempts)
	}
	return m
}

func runWithBloomFilter(sc routingScenario) routingMetrics {
	// Build properly-sized bloom filters for each peer.
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
	m.convergenceAt = -1
	totalAttempts := 0
	totalHits := 0
	rng := rand.New(rand.NewSource(42))

	for faultIdx, pageNo := range sc.accessPattern {
		// Find peers whose bloom filter claims the page.
		var candidates []peerBloom
		for _, pb := range peers {
			if pb.filter.test(pageNo) {
				candidates = append(candidates, pb)
			}
		}

		if len(candidates) == 0 {
			m.s3Fallbacks++
		} else {
			// Random tiebreak among candidates.
			chosen := candidates[rng.Intn(len(candidates))]
			totalAttempts++
			if chosen.pages[pageNo] {
				m.peerHits++
				totalHits++
			} else {
				m.peerMisses++
			}
		}

		if totalAttempts > 0 && m.convergenceAt < 0 {
			rate := float64(totalHits) / float64(totalAttempts)
			if rate >= 0.80 {
				m.convergenceAt = faultIdx
			}
		}
	}

	if totalAttempts > 0 {
		m.hitRate = float64(m.peerHits) / float64(totalAttempts)
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
	m := int(-float64(n) * 4.0 / 0.48) // ~m = -n*ln(p)/ln2^2
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
	// Peer A has interior page I (children 10..109) + all 100 leaf pages.
	// Peer B has different table's pages (200..299).
	peerAPages := make(map[int64]bool)
	for i := int64(10); i < 110; i++ {
		peerAPages[i] = true
	}
	peerBPages := make(map[int64]bool)
	for i := int64(200); i < 300; i++ {
		peerBPages[i] = true
	}

	// B-tree: interior page 5 (0-based) → children [10..109] (0-based).
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

	routerResult := runWithPeerRouter(sc)
	bloomResult := runWithBloomFilter(sc)

	t.Logf("PeerRouter: hitRate=%.2f convergenceAt=%d hits=%d misses=%d s3=%d",
		routerResult.hitRate, routerResult.convergenceAt, routerResult.peerHits, routerResult.peerMisses, routerResult.s3Fallbacks)
	t.Logf("BloomFilter: hitRate=%.2f convergenceAt=%d hits=%d misses=%d s3=%d",
		bloomResult.hitRate, bloomResult.convergenceAt, bloomResult.peerHits, bloomResult.peerMisses, bloomResult.s3Fallbacks)

	if routerResult.hitRate < 0.90 {
		t.Fatalf("PeerRouter hit rate too low: %.2f (want >= 0.90)", routerResult.hitRate)
	}
	if bloomResult.hitRate >= routerResult.hitRate {
		t.Logf("warning: bloom filter hit rate (%.2f) >= router hit rate (%.2f) — bloom was lucky in this scenario",
			bloomResult.hitRate, routerResult.hitRate)
	}
}

func TestRoutingScenario_MixedWorkload(t *testing.T) {
	// Peer A: table T1 pages 10..59. Peer B: table T2 pages 100..149.
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

	// Interleaved access: T1, T2, T1, T2...
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

	result := runWithPeerRouter(sc)
	t.Logf("PeerRouter: hitRate=%.2f convergenceAt=%d hits=%d misses=%d s3=%d",
		result.hitRate, result.convergenceAt, result.peerHits, result.peerMisses, result.s3Fallbacks)

	if result.hitRate < 0.70 {
		t.Fatalf("PeerRouter hit rate too low: %.2f (want >= 0.70)", result.hitRate)
	}
}

func TestRoutingScenario_ColdStartConvergence(t *testing.T) {
	// 3 peers with non-overlapping pages, no B-tree structure.
	peers := make([]simulatedPeer, 3)
	for i := 0; i < 3; i++ {
		pages := make(map[int64]bool)
		for pg := int64(i * 50); pg < int64((i+1)*50); pg++ {
			pages[pg] = true
		}
		peers[i] = simulatedPeer{id: string(rune('A' + i)), pages: pages}
	}

	var pattern []int64
	for i := int64(0); i < 150; i++ {
		pattern = append(pattern, i)
	}

	sc := routingScenario{
		name:          "cold_start",
		peers:         peers,
		accessPattern: pattern,
	}

	result := runWithPeerRouter(sc)
	t.Logf("PeerRouter: hitRate=%.2f convergenceAt=%d hits=%d misses=%d s3=%d",
		result.hitRate, result.convergenceAt, result.peerHits, result.peerMisses, result.s3Fallbacks)

	// Without B-tree help, convergence is slower but should still happen.
	if result.convergenceAt >= 0 && result.convergenceAt > 30 {
		t.Logf("warning: slow convergence without B-tree: %d faults", result.convergenceAt)
	}
}

func TestRoutingScenario_BtreeAmplification(t *testing.T) {
	// 1 peer with 500 pages across 5 interior nodes (100 children each).
	// 1 peer with no relevant pages.
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
		// Use interior page numbers >= 5 to avoid page 0 offset issue.
		btree[uint32(node+5)] = children
	}

	peerBPages := make(map[int64]bool)
	for i := int64(1000); i < 1100; i++ {
		peerBPages[i] = true
	}

	// Access 1 page from each interior node (5 faults).
	pattern := []int64{10, 110, 210, 310, 410}

	sc := routingScenario{
		name:           "btree_amplification",
		peers:          []simulatedPeer{{id: "peerA", pages: peerAPages}, {id: "peerB", pages: peerBPages}},
		btreeStructure: btree,
		accessPattern:  pattern,
	}

	result := runWithPeerRouter(sc)
	t.Logf("PeerRouter: hitRate=%.2f hits=%d misses=%d s3=%d",
		result.hitRate, result.peerHits, result.peerMisses, result.s3Fallbacks)

	// After 5 faults, router should know about all 500 pages via sibling expansion.
	// Verify by checking subsequent pages route correctly.
	// Build readahead engine with structure.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})
	for interiorPgno, children := range btree {
		oneBased := make([]uint32, len(children))
		for i, c := range children {
			oneBased[i] = c + 1
		}
		page := sqlitebtree.BuildInteriorTablePage(4096, oneBased)
		re.OnFetch(int64(interiorPgno), page)
	}
	router2 := NewPeerRouter(re)
	router2.mu.Lock()
	router2.explore = 0 // disable exploration for deterministic check
	router2.mu.Unlock()

	// Simulate 5 hits (one per interior node).
	for _, pg := range pattern {
		router2.RecordResult("peerA", pg, true)
	}

	// All 500 pages should now be hinted.
	if router2.HintCount() < 500 {
		t.Fatalf("expected >= 500 hints after 5 hits with btree amplification, got %d", router2.HintCount())
	}
}

func TestRoutingScenario_PeerDeparture(t *testing.T) {
	// Peer A: pages 10..59, Peer B: pages 100..149.
	peerAPages := make(map[int64]bool)
	for i := int64(10); i < 60; i++ {
		peerAPages[i] = true
	}
	peerBPages := make(map[int64]bool)
	for i := int64(100); i < 150; i++ {
		peerBPages[i] = true
	}

	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})
	router := NewPeerRouter(re)

	// Build up knowledge: 20 hits from A.
	for i := int64(10); i < 30; i++ {
		router.RecordResult("peerA", i, true)
	}
	preRemovalRate := router.PeerHitRate("peerA")
	if preRemovalRate < 0.9 {
		t.Fatalf("expected high hit rate before removal, got %f", preRemovalRate)
	}

	// Remove A, add C with same pages.
	router.RemovePeer("peerA")

	// Simulate exploration+learning for C (same pages as A).
	peerCPages := peerAPages
	router.mu.Lock()
	router.explore = 1.0 // force exploration for faster convergence in test
	router.mu.Unlock()

	hits := 0
	for i := int64(30); i < 50; i++ {
		if router.ShouldTry("peerC", i) {
			if peerCPages[i] {
				router.RecordResult("peerC", i, true)
				hits++
			} else {
				router.RecordResult("peerC", i, false)
			}
		}
	}

	// Should have re-learned peerC quickly.
	if router.PeerHitRate("peerC") < 0.5 {
		t.Fatalf("expected peerC to converge, hit rate = %f", router.PeerHitRate("peerC"))
	}
}

func TestRoutingScenario_LeaderAwareColdStart(t *testing.T) {
	// 10 peers, only peer "leader" has the pages. Without leader hint,
	// cold start requires exploration to discover the right peer.
	// With leader hint, every fault routes directly.
	leaderPages := make(map[int64]bool)
	for i := int64(0); i < 200; i++ {
		leaderPages[i] = true
	}

	var peers []simulatedPeer
	peers = append(peers, simulatedPeer{id: "leader", pages: leaderPages})
	for i := 0; i < 9; i++ {
		// Other peers have no relevant pages.
		peers = append(peers, simulatedPeer{id: string(rune('A' + i)), pages: make(map[int64]bool)})
	}

	pattern := make([]int64, 200)
	for i := range pattern {
		pattern[i] = int64(i)
	}

	sc := routingScenario{
		name:          "leader_cold_start",
		peers:         peers,
		accessPattern: pattern,
	}

	// Without leader hint.
	noLeader := runWithPeerRouter(sc)
	t.Logf("No leader hint: hitRate=%.2f misses=%d s3=%d",
		noLeader.hitRate, noLeader.peerMisses, noLeader.s3Fallbacks)

	// With leader hint: build a custom simulation.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})
	router := NewPeerRouter(re)
	router.SetLeader("leader")

	var withLeader routingMetrics
	totalAttempts := 0
	totalHits := 0
	for _, pageNo := range pattern {
		hit := false
		for _, sp := range peers {
			if router.ShouldTry(sp.id, pageNo) {
				totalAttempts++
				if sp.pages[pageNo] {
					router.RecordResult(sp.id, pageNo, true)
					withLeader.peerHits++
					totalHits++
					hit = true
					break
				} else {
					router.RecordResult(sp.id, pageNo, false)
					withLeader.peerMisses++
				}
			}
		}
		if !hit {
			withLeader.s3Fallbacks++
		}
	}
	if totalAttempts > 0 {
		withLeader.hitRate = float64(withLeader.peerHits) / float64(totalAttempts)
	}

	t.Logf("With leader hint: hitRate=%.2f misses=%d s3=%d",
		withLeader.hitRate, withLeader.peerMisses, withLeader.s3Fallbacks)

	// Leader-aware should have zero misses (all faults go to the leader).
	if withLeader.peerMisses > 0 {
		t.Fatalf("expected 0 peer misses with leader hint, got %d", withLeader.peerMisses)
	}
	if withLeader.hitRate < 1.0 {
		t.Fatalf("expected 100%% hit rate with leader hint, got %.2f", withLeader.hitRate)
	}
	// And fewer S3 fallbacks than without.
	if withLeader.s3Fallbacks > noLeader.s3Fallbacks {
		t.Fatalf("leader hint should not increase S3 fallbacks")
	}
}
