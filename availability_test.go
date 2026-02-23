package prefetch

import (
	"sync"
	"testing"
)

// mockChildLookup implements ChildLookup for testing.
type mockChildLookup struct {
	mu       sync.Mutex
	children map[uint32][]uint32
}

func newMockChildLookup() *mockChildLookup {
	return &mockChildLookup{children: make(map[uint32][]uint32)}
}

func (m *mockChildLookup) SetChildren(interiorPage uint32, children []uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.children[interiorPage] = children
}

func (m *mockChildLookup) Children(interiorPage uint32) ([]uint32, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.children[interiorPage]
	if !ok {
		return nil, false
	}
	result := make([]uint32, len(c))
	copy(result, c)
	return result, true
}

// --- AvailabilityIndex tests ---

func TestAvailabilityIndex_HasPage_NotResolved(t *testing.T) {
	lookup := newMockChildLookup()
	ai := NewAvailabilityIndex(lookup)

	// Apply delta before interior page is parsed — should not resolve.
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false before interior page is parsed")
	}
}

func TestAvailabilityIndex_ApplyDelta_ThenParse(t *testing.T) {
	lookup := newMockChildLookup()
	// Interior page 5 has children [10, 20, 30, 40, 50].
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(lookup)

	// Apply delta (opaque).
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}}, // children[0..2] = 10, 20, 30
	})

	// Not yet resolved.
	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false before parse")
	}

	// Parse the interior page → resolves extents.
	ai.OnInteriorPageParsed(5)

	// Now children 10, 20, 30 should resolve.
	for _, pg := range []uint32{10, 20, 30} {
		if !ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=true after parse", pg)
		}
	}
	// Children 40, 50 not in extent.
	for _, pg := range []uint32{40, 50} {
		if ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=false", pg)
		}
	}
}

func TestAvailabilityIndex_ParseFirst_ThenDelta(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(lookup)

	// Parse first.
	ai.OnInteriorPageParsed(5)

	// Apply delta → should resolve immediately.
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 1, Count: 2}}, // children[1..2] = 20, 30
	})

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage(peer1, 10)=false")
	}
	if !ai.HasPage("peer1", 20) {
		t.Fatal("expected HasPage(peer1, 20)=true")
	}
	if !ai.HasPage("peer1", 30) {
		t.Fatal("expected HasPage(peer1, 30)=true")
	}
}

func TestAvailabilityIndex_MultiplePeers(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)

	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})
	ai.ApplyDelta("peer2", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 2, Count: 3}}, // children[2..4] = 30, 40, 50
	})

	// Page 30 is claimed by both peers.
	if !ai.HasPage("peer1", 30) {
		t.Fatal("expected peer1 has page 30")
	}
	if !ai.HasPage("peer2", 30) {
		t.Fatal("expected peer2 has page 30")
	}
	// Page 10 is only peer1.
	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected peer1 has page 10")
	}
	if ai.HasPage("peer2", 10) {
		t.Fatal("expected peer2 does NOT have page 10")
	}
	// Page 50 is only peer2.
	if ai.HasPage("peer1", 50) {
		t.Fatal("expected peer1 does NOT have page 50")
	}
	if !ai.HasPage("peer2", 50) {
		t.Fatal("expected peer2 has page 50")
	}
}

func TestAvailabilityIndex_RemovePeer(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)

	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=true before remove")
	}

	ai.RemovePeer("peer1")

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false after remove")
	}
}

func TestAvailabilityIndex_DeltaRemove(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)

	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 5}},
	})

	// Remove children[1..2] = 20, 30.
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaRemove,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 1, Count: 2}},
	})

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected page 10 still available")
	}
	if ai.HasPage("peer1", 20) {
		t.Fatal("expected page 20 removed")
	}
	if ai.HasPage("peer1", 30) {
		t.Fatal("expected page 30 removed")
	}
	if !ai.HasPage("peer1", 40) {
		t.Fatal("expected page 40 still available")
	}
}

func TestAvailabilityIndex_ApplySnapshot(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	lookup.SetChildren(6, []uint32{100, 200, 300})
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)
	ai.OnInteriorPageParsed(6)

	// Initial state.
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})

	// Snapshot replaces everything.
	ai.ApplySnapshot("peer1", []PageAvailability{
		{InteriorPage: 6, Extents: []ChildExtent{{Start: 0, Count: 2}}},
	})

	// Old page 5 entries should be gone.
	if ai.HasPage("peer1", 10) {
		t.Fatal("expected old page 10 gone after snapshot")
	}
	// New page 6 entries should be present.
	if !ai.HasPage("peer1", 100) {
		t.Fatal("expected page 100 from snapshot")
	}
	if !ai.HasPage("peer1", 200) {
		t.Fatal("expected page 200 from snapshot")
	}
	if ai.HasPage("peer1", 300) {
		t.Fatal("expected page 300 NOT in snapshot")
	}
}

func TestAvailabilityIndex_Reset(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})

	ai.Reset()

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false after reset")
	}
}

func TestAvailabilityIndex_SnapshotBeforeParse(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(lookup)

	// Snapshot before parse — should be stored opaquely.
	ai.ApplySnapshot("peer1", []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 3}}},
	})

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false before parse")
	}

	// Now parse → resolve.
	ai.OnInteriorPageParsed(5)

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=true after parse")
	}
}

func TestAvailabilityIndex_MultipleInteriorPages(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	lookup.SetChildren(6, []uint32{100, 200, 300})
	ai := NewAvailabilityIndex(lookup)

	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 3}},
	})
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 6,
		Extents:      []ChildExtent{{Start: 1, Count: 2}},
	})

	// Parse page 5 only.
	ai.OnInteriorPageParsed(5)

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected page 10 resolved")
	}
	if ai.HasPage("peer1", 200) {
		t.Fatal("expected page 200 NOT resolved (page 6 not parsed)")
	}

	// Parse page 6.
	ai.OnInteriorPageParsed(6)

	if !ai.HasPage("peer1", 200) {
		t.Fatal("expected page 200 resolved after parsing page 6")
	}
}

func TestAvailabilityIndex_ExtentOutOfBounds(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30}) // 3 children
	ai := NewAvailabilityIndex(lookup)
	ai.OnInteriorPageParsed(5)

	// Extent goes past end of children array — should be clamped.
	ai.ApplyDelta("peer1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 1, Count: 100}}, // way past end
	})

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected page 10 NOT in extent (starts at idx 1)")
	}
	if !ai.HasPage("peer1", 20) {
		t.Fatal("expected page 20 in extent")
	}
	if !ai.HasPage("peer1", 30) {
		t.Fatal("expected page 30 in extent (clamped)")
	}
}

// --- LocalAvailability tests ---

func TestLocalAvailability_OnPageCached(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	la := NewLocalAvailability(lookup)

	la.OnInteriorPageParsed(5, []uint32{10, 20, 30, 40, 50})

	var deltas []AvailabilityDelta
	var mu sync.Mutex
	la.OnChange(func(d AvailabilityDelta) {
		mu.Lock()
		deltas = append(deltas, d)
		mu.Unlock()
	})

	la.OnPageCached(10)
	la.OnPageCached(20)
	la.OnPageCached(30)

	mu.Lock()
	defer mu.Unlock()
	if len(deltas) != 3 {
		t.Fatalf("expected 3 deltas, got %d", len(deltas))
	}
	// Last delta should have merged extent [0, 3].
	last := deltas[2]
	if last.InteriorPage != 5 {
		t.Fatalf("expected interior page 5, got %d", last.InteriorPage)
	}
	if len(last.Extents) != 1 {
		t.Fatalf("expected 1 extent, got %d", len(last.Extents))
	}
	if last.Extents[0].Start != 0 || last.Extents[0].Count != 3 {
		t.Fatalf("expected extent {0, 3}, got {%d, %d}", last.Extents[0].Start, last.Extents[0].Count)
	}
}

func TestLocalAvailability_OnPageCached_UnknownPage(t *testing.T) {
	lookup := newMockChildLookup()
	la := NewLocalAvailability(lookup)

	var deltas []AvailabilityDelta
	la.OnChange(func(d AvailabilityDelta) {
		deltas = append(deltas, d)
	})

	// Unknown page — should be silently ignored.
	la.OnPageCached(999)

	if len(deltas) != 0 {
		t.Fatal("expected no delta for unknown page")
	}
}

func TestLocalAvailability_Snapshot(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(lookup)
	la.OnInteriorPageParsed(5, []uint32{10, 20, 30})

	la.OnPageCached(10)
	la.OnPageCached(30)

	snap := la.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 page in snapshot, got %d", len(snap))
	}
	if snap[0].InteriorPage != 5 {
		t.Fatalf("expected interior page 5, got %d", snap[0].InteriorPage)
	}
	// Should have 2 extents: {0,1} and {2,1} (non-contiguous).
	if len(snap[0].Extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(snap[0].Extents))
	}
}

func TestLocalAvailability_Reset(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(lookup)
	la.OnInteriorPageParsed(5, []uint32{10, 20, 30})
	la.OnPageCached(10)

	la.Reset()

	snap := la.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot after reset, got %d", len(snap))
	}
}

func TestLocalAvailability_DuplicateCache(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(lookup)
	la.OnInteriorPageParsed(5, []uint32{10, 20, 30})

	var count int
	la.OnChange(func(d AvailabilityDelta) {
		count++
	})

	la.OnPageCached(10)
	la.OnPageCached(10) // duplicate

	if count != 1 {
		t.Fatalf("expected 1 delta (duplicate should be ignored), got %d", count)
	}
}

func TestLocalAvailability_DiscontiguousExtents(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})
	la := NewLocalAvailability(lookup)
	la.OnInteriorPageParsed(5, []uint32{10, 20, 30, 40, 50})

	la.OnPageCached(10)
	la.OnPageCached(30)
	la.OnPageCached(50)

	snap := la.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 page in snapshot, got %d", len(snap))
	}
	// Indices 0, 2, 4 → three separate extents.
	if len(snap[0].Extents) != 3 {
		t.Fatalf("expected 3 extents, got %d: %+v", len(snap[0].Extents), snap[0].Extents)
	}
}
