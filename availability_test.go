package prefetch

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// --- AvailabilityIndex tests ---

func TestAvailabilityIndex_HasPage_NoPeer(t *testing.T) {
	ai := NewAvailabilityIndex()

	ai.ApplyDelta("peer1", DeltaAdd, 10)

	if !ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=true after delta add")
	}
	if ai.HasPage("peer2", 10) {
		t.Fatal("expected HasPage=false for unknown peer")
	}
}

func TestAvailabilityIndex_ApplyDelta(t *testing.T) {
	ai := NewAvailabilityIndex()

	ai.ApplyDelta("peer1", DeltaAdd, 10)
	ai.ApplyDelta("peer1", DeltaAdd, 20)
	ai.ApplyDelta("peer1", DeltaAdd, 30)

	for _, pg := range []uint32{10, 20, 30} {
		if !ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=true", pg)
		}
	}
	for _, pg := range []uint32{40, 50} {
		if ai.HasPage("peer1", pg) {
			t.Fatalf("expected HasPage(peer1, %d)=false", pg)
		}
	}
}

func TestAvailabilityIndex_MultiplePeers(t *testing.T) {
	ai := NewAvailabilityIndex()

	// peer1: pages 10, 20, 30
	for _, pg := range []uint32{10, 20, 30} {
		ai.ApplyDelta("peer1", DeltaAdd, pg)
	}
	// peer2: pages 30, 40, 50
	for _, pg := range []uint32{30, 40, 50} {
		ai.ApplyDelta("peer2", DeltaAdd, pg)
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
	ai := NewAvailabilityIndex()

	for _, pg := range []uint32{10, 20, 30} {
		ai.ApplyDelta("peer1", DeltaAdd, pg)
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
	ai := NewAvailabilityIndex()

	for _, pg := range []uint32{10, 20, 30, 40, 50} {
		ai.ApplyDelta("peer1", DeltaAdd, pg)
	}

	ai.ApplyDelta("peer1", DeltaRemove, 20)
	ai.ApplyDelta("peer1", DeltaRemove, 30)

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
	ai := NewAvailabilityIndex()

	// Initial state: peer1 has pages 10, 20, 30.
	for _, pg := range []uint32{10, 20, 30} {
		ai.ApplyDelta("peer1", DeltaAdd, pg)
	}

	// Snapshot replaces everything: peer1 now only has pages 100, 200.
	bm := roaring64.New()
	bm.Add(100)
	bm.Add(200)
	snapData, _ := bm.MarshalBinary()
	if err := ai.ApplySnapshot("peer1", snapData); err != nil {
		t.Fatal(err)
	}

	// Old entries should be gone.
	if ai.HasPage("peer1", 10) {
		t.Fatal("expected old page 10 gone after snapshot")
	}
	// New entries should be present.
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
	ai := NewAvailabilityIndex()

	for _, pg := range []uint32{10, 20, 30} {
		ai.ApplyDelta("peer1", DeltaAdd, pg)
	}

	ai.Reset()

	if ai.HasPage("peer1", 10) {
		t.Fatal("expected HasPage=false after reset")
	}
}

func TestAvailabilityIndex_EmptySnapshot(t *testing.T) {
	ai := NewAvailabilityIndex()

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
	la := NewLocalAvailability()

	var deltas []uint32
	var mu sync.Mutex
	la.OnChange(func(op DeltaOp, pageNo uint32) {
		mu.Lock()
		deltas = append(deltas, pageNo)
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
	if deltas[0] != 10 {
		t.Fatalf("expected page 10, got %d", deltas[0])
	}
	if deltas[2] != 30 {
		t.Fatalf("expected page 30, got %d", deltas[2])
	}
}

func TestLocalAvailability_OnPageCached_AllTracked(t *testing.T) {
	la := NewLocalAvailability()

	var count int
	la.OnChange(func(op DeltaOp, pageNo uint32) {
		count++
	})

	// All pages are tracked (no addrMap filtering).
	la.OnPageCached(999)

	if count != 1 {
		t.Fatalf("expected 1 delta, got %d", count)
	}
}

func TestLocalAvailability_Snapshot(t *testing.T) {
	la := NewLocalAvailability()

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
	if !bm.Contains(10) {
		t.Fatal("expected snapshot to contain page 10")
	}
	if !bm.Contains(30) {
		t.Fatal("expected snapshot to contain page 30")
	}
}

func TestLocalAvailability_Reset(t *testing.T) {
	la := NewLocalAvailability()
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
	la := NewLocalAvailability()

	var count int
	la.OnChange(func(op DeltaOp, pageNo uint32) {
		count++
	})

	la.OnPageCached(10)
	la.OnPageCached(10) // duplicate

	if count != 1 {
		t.Fatalf("expected 1 delta (duplicate should be ignored), got %d", count)
	}
}

func TestLocalAvailability_DiscontiguousPages(t *testing.T) {
	la := NewLocalAvailability()

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
	if bm.GetCardinality() != 3 {
		t.Fatalf("expected 3 entries, got %d", bm.GetCardinality())
	}
}
