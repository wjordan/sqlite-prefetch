package prefetch

import (
	"testing"

	"github.com/wjordan/sqlite-prefetch/pagefault"
	"github.com/wjordan/sqlite-prefetch/sqlitebtree"
)

func TestPeerSource_Properties(t *testing.T) {
	// Build a btree tracker and availability index.
	pf := pagefault.New(&nullSource{}, &nullCache{})
	re := NewReadaheadEngine(pf, &nullCache{}, ReadaheadConfig{})

	// Feed interior page 2: 1-based [2, 6] → 0-based [1, 5].
	children := []uint32{2, 6}
	page := sqlitebtree.BuildInteriorTablePage(4096, children)
	re.OnFetch(2, page)

	ai := NewAvailabilityIndex(re.Btree())
	ai.OnInteriorPageParsed(2)

	// Pre-populate availability for node-1.
	ai.ApplyDelta("node-1", AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 2,
		Extents:      []ChildExtent{{Start: 0, Count: 2}}, // children[0..1] = 1, 5
	})

	ps := NewPeerSource(nil, "node-1", "10.0.0.2:9001", ai, PeerSourceConfig{})

	if got := ps.Name(); got != "peer:node-1" {
		t.Fatalf("Name() = %q, want %q", got, "peer:node-1")
	}
	if got := ps.NodeID(); got != "node-1" {
		t.Fatalf("NodeID() = %q, want %q", got, "node-1")
	}
	if got := ps.Addr(); got != "10.0.0.2:9001" {
		t.Fatalf("Addr() = %q, want %q", got, "10.0.0.2:9001")
	}
	// Availability-based page checks.
	if !ps.HasPage(1) {
		t.Fatal("HasPage(1) = false, want true")
	}
	if !ps.HasPage(5) {
		t.Fatal("HasPage(5) = false, want true")
	}
	// Page not in availability.
	if ps.HasPage(99) {
		t.Fatal("HasPage(99) = true, want false")
	}

	// Nil avail returns false.
	psNil := NewPeerSource(nil, "node-2", "10.0.0.3:9001", nil, PeerSourceConfig{})
	if psNil.HasPage(1) {
		t.Fatal("HasPage with nil avail should return false")
	}

	// Default latency and bandwidth should be set.
	if lat := ps.Latency(); lat <= 0 {
		t.Fatalf("Latency() = %v, want > 0", lat)
	}
	if bw := ps.Bandwidth(); bw <= 0 {
		t.Fatalf("Bandwidth() = %f, want > 0", bw)
	}
}
