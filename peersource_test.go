package prefetch

import (
	"testing"
)

func TestPeerSource_Properties(t *testing.T) {
	router := NewPeerRouter(nil)
	// Pre-populate router with hints for testing HasPage.
	router.RecordResult("node-1", 1, true)
	router.RecordResult("node-1", 5, true)

	ps := NewPeerSource(nil, "node-1", "10.0.0.2:9001", router, PeerSourceConfig{})

	if got := ps.Name(); got != "peer:node-1" {
		t.Fatalf("Name() = %q, want %q", got, "peer:node-1")
	}
	if got := ps.NodeID(); got != "node-1" {
		t.Fatalf("NodeID() = %q, want %q", got, "node-1")
	}
	if got := ps.Addr(); got != "10.0.0.2:9001" {
		t.Fatalf("Addr() = %q, want %q", got, "10.0.0.2:9001")
	}
	// Router-based page checks.
	if !ps.HasPage(1) {
		t.Fatal("HasPage(1) = false, want true")
	}
	if !ps.HasPage(5) {
		t.Fatal("HasPage(5) = false, want true")
	}

	// Nil router returns false.
	psNil := NewPeerSource(nil, "node-2", "10.0.0.3:9001", nil, PeerSourceConfig{})
	if psNil.HasPage(1) {
		t.Fatal("HasPage with nil router should return false")
	}

	// Default latency and bandwidth should be set.
	if lat := ps.Latency(); lat <= 0 {
		t.Fatalf("Latency() = %v, want > 0", lat)
	}
	if bw := ps.Bandwidth(); bw <= 0 {
		t.Fatalf("Bandwidth() = %f, want > 0", bw)
	}
}
