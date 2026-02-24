package prefetch

import (
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// mockGossipMesh records all sends for testing.
type mockGossipMesh struct {
	mu        sync.Mutex
	datagrams []meshMessage
	streams   []meshMessage
	livePeers []string
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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(addrMap)
	local.OnPageCached(10)
	local.OnPageCached(20)

	remote := NewAvailabilityIndex(addrMap)
	mesh := newMockGossipMesh("peer1", "peer2")

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour, // disable periodic sync for test
	})
	_ = g

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
	bitmapData, err := DecodeSnapshot(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	bm := roaring64.New()
	if err := bm.UnmarshalBinary(bitmapData); err != nil {
		t.Fatal(err)
	}
	if bm.GetCardinality() != 2 {
		t.Fatalf("expected 2 entries in snapshot, got %d", bm.GetCardinality())
	}
}

func TestAvailabilityGossip_DeltaBroadcast(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(addrMap)

	remote := NewAvailabilityIndex(addrMap)
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
	op, logAddr, err := DecodeDelta(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	if op != DeltaAdd {
		t.Fatalf("expected DeltaAdd, got %d", op)
	}
	if logAddr != logicalAddr(5, 0) {
		t.Fatalf("expected logicalAddr(5,0)=%d, got %d", logicalAddr(5, 0), logAddr)
	}
}

func TestAvailabilityGossip_HandleSnapshot(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(addrMap)
	remote := NewAvailabilityIndex(addrMap)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Build and handle a snapshot from peer1.
	bm := roaring64.New()
	for i := 0; i < 3; i++ {
		bm.Add(logicalAddr(5, i))
	}
	bitmapData, _ := bm.MarshalBinary()
	snap := EncodeSnapshot(bitmapData)
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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(addrMap)
	remote := NewAvailabilityIndex(addrMap)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Handle a delta from peer1: add children[1] and children[2].
	deltaData1 := EncodeDelta(DeltaAdd, logicalAddr(5, 1))
	if err := g.HandleDelta("peer1", deltaData1); err != nil {
		t.Fatal(err)
	}
	deltaData2 := EncodeDelta(DeltaAdd, logicalAddr(5, 2))
	if err := g.HandleDelta("peer1", deltaData2); err != nil {
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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(addrMap)
	remote := NewAvailabilityIndex(addrMap)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// HandleMessage with snapshot.
	bm := roaring64.New()
	for i := 0; i < 3; i++ {
		bm.Add(logicalAddr(5, i))
	}
	bitmapData, _ := bm.MarshalBinary()
	snapData := EncodeSnapshot(bitmapData)
	if err := g.HandleMessage("peer1", snapData); err != nil {
		t.Fatal(err)
	}
	if !remote.HasPage("peer1", 10) {
		t.Fatal("expected HandleMessage to dispatch snapshot")
	}

	// HandleMessage with delta (remove child 0 = page 10).
	deltaData := EncodeDelta(DeltaRemove, logicalAddr(5, 0))
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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})

	local := NewLocalAvailability(addrMap)
	remote := NewAvailabilityIndex(addrMap)
	mesh := newMockGossipMesh()

	g := NewAvailabilityGossip(mesh, local, remote, AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})

	// Add peer1 availability.
	bm := roaring64.New()
	for i := 0; i < 3; i++ {
		bm.Add(logicalAddr(5, i))
	}
	snapData, _ := bm.MarshalBinary()
	remote.ApplySnapshot("peer1", snapData)

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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30, 40, 50})

	// Node A setup.
	localA := NewLocalAvailability(addrMap)

	// Node B setup.
	remoteB := NewAvailabilityIndex(addrMap)

	meshA := newMockGossipMesh("nodeB")
	gossipA := NewAvailabilityGossip(meshA, localA, NewAvailabilityIndex(addrMap), AvailabilityGossipConfig{
		FullSyncInterval: time.Hour,
	})
	meshB := newMockGossipMesh("nodeA")
	gossipB := NewAvailabilityGossip(meshB, NewLocalAvailability(addrMap), remoteB, AvailabilityGossipConfig{
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
	addrMap := NewLogicalAddressMap()
	addrMap.Register(5, []uint32{10, 20, 30})
	local := NewLocalAvailability(addrMap)
	local.OnPageCached(10)

	remote := NewAvailabilityIndex(addrMap)
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
	bitmapData, err := DecodeSnapshot(msg.data)
	if err != nil {
		t.Fatal(err)
	}
	bm := roaring64.New()
	if err := bm.UnmarshalBinary(bitmapData); err != nil {
		t.Fatal(err)
	}
	if bm.GetCardinality() != 1 {
		t.Fatalf("expected 1 entry in snapshot, got %d", bm.GetCardinality())
	}
}

func TestAvailabilityGossip_StartStop(t *testing.T) {
	addrMap := NewLogicalAddressMap()
	local := NewLocalAvailability(addrMap)
	remote := NewAvailabilityIndex(addrMap)
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
