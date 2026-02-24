package prefetch

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// logicalAddr computes the logical address for a child at the given index
// under an interior page.
func logicalAddr(interiorPage uint32, childIdx int) uint64 {
	return uint64(interiorPage)*LogicalStride + uint64(childIdx)
}

// --- AvailabilityIndex tests ---

func TestAvailabilityIndex_HasPage_NotRegistered(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	// Don't register anything — addrMap.Lookup will fail.
	ai := NewAvailabilityIndex(addrMap)

	ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, 0))

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false when address map not populated")
	}
}

func TestAvailabilityIndex_ApplyDelta_ThenRegister(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	ai := NewAvailabilityIndex(addrMap)

	// Apply deltas for children[0..2] of interior page 5.
	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}

	// Not yet registered — HasPage returns false.
	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false before register")
	}

	// Register the interior page → resolves lookups.
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})

	// Now children 10, 20, 30 should be found.
	for _, pg := range []uint32{10, 20, 30} {
		if !ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=true after register", pg)
		}
	}
	// Children 40, 50 not in bitmap.
	for _, pg := range []uint32{40, 50} {
		if ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=false", pg)
		}
	}
}

func TestAvailabilityIndex_RegisterFirst_ThenDelta(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(addrMap)

	// Apply delta → should be queryable immediately.
	ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, 1)) // child 20
	ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, 2)) // child 30

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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(addrMap)

	// peer1: children[0..2] = 10, 20, 30
	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}
	// peer2: children[2..4] = 30, 40, 50
	for i := 2; i < 5; i++ {
		ai.ApplyDelta("peer2", DeltaAdd, logicalAddr(5, i))
	}

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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(addrMap)

	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=true before remove")
	}

	ai.RemovePeer("peer1")

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false after remove")
	}
}

func TestAvailabilityIndex_DeltaRemove(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})
	ai := NewAvailabilityIndex(addrMap)

	// Add all 5 children.
	for i := 0; i < 5; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}

	// Remove children[1..2] = 20, 30.
	ai.ApplyDelta("peer1", DeltaRemove, logicalAddr(5, 1))
	ai.ApplyDelta("peer1", DeltaRemove, logicalAddr(5, 2))

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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	addrMap.Register(6, []uint32{100, 200, 300})
	ai := NewAvailabilityIndex(addrMap)

	// Initial state: peer1 has all of page 5's children.
	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}

	// Snapshot replaces everything: peer1 now only has page 6 children[0..1].
	bm := roaring64.New()
	bm.Add(logicalAddr(6, 0))
	bm.Add(logicalAddr(6, 1))
	snapData, _ := bm.MarshalBinary()
	if err := ai.ApplySnapshot("peer1", snapData); err != nil {
		t.Fatal(err)
	}

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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(addrMap)

	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}

	ai.Reset()

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false after reset")
	}
}

func TestAvailabilityIndex_SnapshotBeforeRegister(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	ai := NewAvailabilityIndex(addrMap)

	// Snapshot before register — bitmap is stored, but lookup fails.
	bm := roaring64.New()
	for i := 0; i < 3; i++ {
		bm.Add(logicalAddr(5, i))
	}
	snapData, _ := bm.MarshalBinary()
	if err := ai.ApplySnapshot("peer1", snapData); err != nil {
		t.Fatal(err)
	}

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false before register")
	}

	// Now register → lookup succeeds.
	addrMap.Register(5, []uint32{10, 20, 30})

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=true after register")
	}
}

func TestAvailabilityIndex_MultipleInteriorPages(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	ai := NewAvailabilityIndex(addrMap)

	// Add deltas for two interior pages.
	for i := 0; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(5, i))
	}
	for i := 1; i < 3; i++ {
		ai.ApplyDelta("peer1", DeltaAdd, logicalAddr(6, i))
	}

	// Register page 5 only.
	addrMap.Register(5, []uint32{10, 20, 30})

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected page 10 resolved")
	}
	if ai.HasPage("peer1", 200) {
		t.Fatal("expected page 200 NOT resolved (page 6 not registered)")
	}

	// Register page 6.
	addrMap.Register(6, []uint32{100, 200, 300})

	if !ai.HasPage("peer1", 200) {
		t.Fatal("expected page 200 resolved after registering page 6")
	}
}

func TestAvailabilityIndex_EmptySnapshot(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	ai := NewAvailabilityIndex(addrMap)

	// Apply an empty snapshot (nil data).
	if err := ai.ApplySnapshot("peer1", nil); err != nil {
		t.Fatal(err)
	}

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false for empty snapshot")
	}
}

// --- LocalAvailability tests ---

func TestLocalAvailability_OnPageCached(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})
	la := NewLocalAvailability(addrMap)

	var deltas []uint64
	var mu sync.Mutex
	la.OnChange(func(op DeltaOp, addr uint64) {
		mu.Lock()
		deltas = append(deltas, addr)
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
	// Each delta is a single logical address add.
	if deltas[0] != logicalAddr(5, 0) {
		t.Fatalf("expected logicalAddr(5,0), got %d", deltas[0])
	}
	if deltas[2] != logicalAddr(5, 2) {
		t.Fatalf("expected logicalAddr(5,2), got %d", deltas[2])
	}
}

func TestLocalAvailability_OnPageCached_UnknownPage(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	la := NewLocalAvailability(addrMap)

	var count int
	la.OnChange(func(op DeltaOp, addr uint64) {
		count++
	})

	// Unknown page — should be silently ignored.
	la.OnPageCached(999)

	if count != 0 {
		t.Fatal("expected no delta for unknown page")
	}
}

func TestLocalAvailability_Snapshot(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(addrMap)

	la.OnPageCached(10)
	la.OnPageCached(30)

	snapData, err := la.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	bm := roaring64.New()
	if err := bm.UnmarshalBinary(snapData); err != nil {
		t.Fatal(err)
	}
	if bm.GetCardinality() != 2 {
		t.Fatalf("expected 2 entries in snapshot, got %d", bm.GetCardinality())
	}
	if !bm.Contains(logicalAddr(5, 0)) {
		t.Fatal("expected snapshot to contain child 0")
	}
	if !bm.Contains(logicalAddr(5, 2)) {
		t.Fatal("expected snapshot to contain child 2")
	}
}

func TestLocalAvailability_Reset(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(addrMap)
	la.OnPageCached(10)

	la.Reset()

	snapData, err := la.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	bm := roaring64.New()
	if err := bm.UnmarshalBinary(snapData); err != nil {
		t.Fatal(err)
	}
	if bm.GetCardinality() != 0 {
		t.Fatalf("expected empty snapshot after reset, got %d", bm.GetCardinality())
	}
}

func TestLocalAvailability_DuplicateCache(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	la := NewLocalAvailability(addrMap)

	var count int
	la.OnChange(func(op DeltaOp, addr uint64) {
		count++
	})

	la.OnPageCached(10)
	la.OnPageCached(10) // duplicate

	if count != 1 {
		t.Fatalf("expected 1 delta (duplicate should be ignored), got %d", count)
	}
}

func TestLocalAvailability_DiscontiguousPages(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})
	la := NewLocalAvailability(addrMap)

	la.OnPageCached(10)
	la.OnPageCached(30)
	la.OnPageCached(50)

	snapData, err := la.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	bm := roaring64.New()
	if err := bm.UnmarshalBinary(snapData); err != nil {
		t.Fatal(err)
	}
	// Indices 0, 2, 4 — three separate entries.
	if bm.GetCardinality() != 3 {
		t.Fatalf("expected 3 entries, got %d", bm.GetCardinality())
	}
}
