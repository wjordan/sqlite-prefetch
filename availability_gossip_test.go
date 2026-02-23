package prefetch

import (
	"sync"
	"testing"
	"time"
)

// mockGossipMesh records all sends for testing.
type mockGossipMesh struct {
	mu         sync.Mutex
	datagrams  []meshMessage
	streams    []meshMessage
	livePeers  []string
}

type meshMessage struct {
	peerID string
	data   []byte
}

func newMockGossipMesh(peers ...string) *mockGossipMesh {
	return &mockGossipMesh{livePeers: peers}
}

func (m *mockGossipMesh) SendDatagram(peerID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.datagrams = append(m.datagrams, meshMessage{peerID: peerID, data: cp})
	return nil
}

func (m *mockGossipMesh) SendStream(peerID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.streams = append(m.streams, meshMessage{peerID: peerID, data: cp})
	return nil
}

func (m *mockGossipMesh) LivePeers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.livePeers))
	copy(cp, m.livePeers)
	return cp
}

func (m *mockGossipMesh) DatagramCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.datagrams)
}

func (m *mockGossipMesh) StreamCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.streams)
}

func (m *mockGossipMesh) GetStream(i int) meshMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[i]
}

func (m *mockGossipMesh) GetDatagram(i int) meshMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.datagrams[i]
}

func TestAvailabilityGossip_OnPeerJoined(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(lookup)
	local.OnInteriorPageParsed(5, []uint32{10, 20, 30})
	local.OnPageCached(10)
	local.OnPageCached(20)

	remote := NewAvailabilityIndex(lookup)
	mesh := newMockGossipMesh("peer1", "peer2")

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour, // disable periodic sync for test
	})
	_ = g // suppress unused

	// Simulate peer join.
	g.OnPeerJoined("peer1")

	if mesh.StreamCount() != 1 {
		t.Fatalf("expected 1 stream send on peer join, got %d", mesh.StreamCount())
	}

	msg := mesh.GetStream(0)
	if msg.peerID != "peer1" {
		t.Fatalf("expected send to peer1, got %s", msg.peerID)
	}

	// Verify the snapshot can be decoded.
	pages, err := DecodeSnapshot(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 1 {
		t.Fatalf("expected 1 page in snapshot, got %d", len(pages))
	}
	if pages[0].InteriorPage != 5 {
		t.Fatalf("expected interior page 5, got %d", pages[0].InteriorPage)
	}
}

func TestAvailabilityGossip_DeltaBroadcast(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(lookup)
	local.OnInteriorPageParsed(5, []uint32{10, 20, 30})

	remote := NewAvailabilityIndex(lookup)
	mesh := newMockGossipMesh("peer1", "peer2")

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})
	_ = g

	// Cache a page — should trigger delta broadcast to all live peers.
	local.OnPageCached(10)

	if mesh.DatagramCount() != 2 {
		t.Fatalf("expected 2 datagrams (1 per peer), got %d", mesh.DatagramCount())
	}

	// Verify delta can be decoded.
	msg := mesh.GetDatagram(0)
	delta, err := DecodeDelta(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Op != DeltaAdd {
		t.Fatalf("expected DeltaAdd, got %d", delta.Op)
	}
	if delta.InteriorPage != 5 {
		t.Fatalf("expected interior page 5, got %d", delta.InteriorPage)
	}
}

func TestAvailabilityGossip_HandleSnapshot(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(lookup)
	remote := NewAvailabilityIndex(lookup)
	remote.OnInteriorPageParsed(5)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Build and handle a snapshot from peer1.
	snap := EncodeSnapshot([]PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 3}}},
	})
	if err := g.HandleSnapshot("peer1", snap); err != nil {
		t.Fatal(err)
	}

	// Remote availability should now know about peer1's pages.
	for _, pg := range []uint32{10, 20, 30} {
		if !remote.HasPage("peer1", pg) {
			t.Fatalf("expected remote to know peer1 has page %d", pg)
		}
	}
}

func TestAvailabilityGossip_HandleDelta(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(lookup)
	remote := NewAvailabilityIndex(lookup)
	remote.OnInteriorPageParsed(5)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Handle a delta from peer1.
	deltaData := EncodeDelta(AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 1, Count: 2}},
	})
	if err := g.HandleDelta("peer1", deltaData); err != nil {
		t.Fatal(err)
	}

	if remote.HasPage("peer1", 10) {
		t.Fatal("expected peer1 NOT to have page 10")
	}
	if !remote.HasPage("peer1", 20) {
		t.Fatal("expected peer1 to have page 20")
	}
	if !remote.HasPage("peer1", 30) {
		t.Fatal("expected peer1 to have page 30")
	}
}

func TestAvailabilityGossip_HandleMessage(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(lookup)
	remote := NewAvailabilityIndex(lookup)
	remote.OnInteriorPageParsed(5)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// HandleMessage with snapshot.
	snapData := EncodeSnapshot([]PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 3}}},
	})
	if err := g.HandleMessage("peer1", snapData); err != nil {
		t.Fatal(err)
	}
	if !remote.HasPage("peer1", 10) {
		t.Fatal("expected HandleMessage to dispatch snapshot")
	}

	// HandleMessage with delta.
	deltaData := EncodeDelta(AvailabilityDelta{
		Op:           DeltaRemove,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 1}},
	})
	if err := g.HandleMessage("peer1", deltaData); err != nil {
		t.Fatal(err)
	}
	if remote.HasPage("peer1", 10) {
		t.Fatal("expected page 10 removed by delta via HandleMessage")
	}

	// HandleMessage with unrelated type.
	if err := g.HandleMessage("peer1", []byte{0xFF, 0x01, 0, 0, 0, 0}); err != nil {
		t.Fatal("expected no error for unrelated message type")
	}
}

func TestAvailabilityGossip_OnPeerLeft(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(lookup)
	remote := NewAvailabilityIndex(lookup)
	remote.OnInteriorPageParsed(5)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Add peer1 availability.
	remote.ApplySnapshot("peer1", []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 3}}},
	})

	if !remote.HasPage("peer1", 10) {
		t.Fatal("expected peer1 has page 10")
	}

	g.OnPeerLeft("peer1")

	if remote.HasPage("peer1", 10) {
		t.Fatal("expected peer1 gone after OnPeerLeft")
	}
}

func TestAvailabilityGossip_FullSyncPropagation(t *testing.T) {
	// End-to-end: node A caches pages, sends gossip to node B,
	// node B can query availability.
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30, 40, 50})

	// Node A setup.
	localA := NewLocalAvailability(lookup)
	localA.OnInteriorPageParsed(5, []uint32{10, 20, 30, 40, 50})

	// Node B setup.
	remoteB := NewAvailabilityIndex(lookup)
	remoteB.OnInteriorPageParsed(5)

	meshA := newMockGossipMesh("nodeB")
	gossipA := NewAvailabilityGossip(meshA, localA, NewAvailabilityIndex(lookup), AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})
	meshB := newMockGossipMesh("nodeA")
	gossipB := NewAvailabilityGossip(meshB, NewLocalAvailability(lookup), remoteB, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})
	_ = gossipA

	// Node A caches pages 10, 20, 30.
	localA.OnPageCached(10)
	localA.OnPageCached(20)
	localA.OnPageCached(30)

	// Simulate: deliver datagrams from A's mesh to B's gossip handler.
	meshA.mu.Lock()
	for _, msg := range meshA.datagrams {
		if msg.peerID == "nodeB" {
			gossipB.HandleMessage("nodeA", msg.data)
		}
	}
	meshA.mu.Unlock()

	// Node B should now know nodeA has pages 10, 20, 30.
	for _, pg := range []uint32{10, 20, 30} {
		if !remoteB.HasPage("nodeA", pg) {
			t.Fatalf("expected nodeB to know nodeA has page %d", pg)
		}
	}
	// But not 40, 50.
	for _, pg := range []uint32{40, 50} {
		if remoteB.HasPage("nodeA", pg) {
			t.Fatalf("expected nodeB NOT to think nodeA has page %d", pg)
		}
	}
}

func TestAvailabilityGossip_PeriodicSync(t *testing.T) {
	lookup := newMockChildLookup()
	lookup.SetChildren(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(lookup)
	local.OnInteriorPageParsed(5, []uint32{10, 20, 30})
	local.OnPageCached(10)

	remote := NewAvailabilityIndex(lookup)
	mesh := newMockGossipMesh("peer1")

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: 50 * time.Millisecond,
	})
	g.Start()
	defer g.Stop()

	// Wait for at least one periodic sync.
	time.Sleep(150 * time.Millisecond)

	if mesh.StreamCount() == 0 {
		t.Fatal("expected at least 1 periodic snapshot stream send")
	}

	// Verify the snapshot is valid.
	msg := mesh.GetStream(0)
	pages, err := DecodeSnapshot(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 1 || pages[0].InteriorPage != 5 {
		t.Fatalf("unexpected snapshot content: %+v", pages)
	}
}

func TestAvailabilityGossip_StartStop(t *testing.T) {
	lookup := newMockChildLookup()
	local := NewLocalAvailability(lookup)
	remote := NewAvailabilityIndex(lookup)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Millisecond,
	})
	g.Start()

	// Stop should return without hanging.
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() hung")
	}
}
