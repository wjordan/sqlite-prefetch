package prefetch

import (
	"sync"
	"time"
)

// GossipMesh is the interface for the gossip transport layer. Implementations
// wrap QUIC or other transport protocols.
type GossipMesh interface {
	// SendDatagram sends an unreliable datagram to a peer. Datagrams may be
	// lost or reordered.
	SendDatagram(peerID string, data []byte) error

	// SendStream sends data reliably to a peer via a unidirectional stream.
	SendStream(peerID string, data []byte) error

	// LivePeers returns the currently connected peer IDs.
	LivePeers() []string
}

// AvailabilityGossipConfig controls gossip exchange behavior.
type AvailabilityGossipConfig struct {
	// FullSyncInterval is how often to send full snapshots to all peers.
	// Default: 30s.
	FullSyncInterval time.Duration
}

func (c *AvailabilityGossipConfig) withDefaults() {
	if c.FullSyncInterval <= 0 {
		c.FullSyncInterval = 30 * time.Second
	}
}

// AvailabilityGossip coordinates availability exchange between peers using
// a GossipMesh transport.
type AvailabilityGossip struct {
	mu sync.Mutex

	mesh   GossipMesh
	local  *LocalAvailability
	remote *AvailabilityIndex
	cfg    AvailabilityGossipConfig

	stopCh chan struct{}
	done   chan struct{}
}

// NewAvailabilityGossip creates an AvailabilityGossip coordinator.
func NewAvailabilityGossip(
	mesh GossipMesh,
	local *LocalAvailability,
	remote *AvailabilityIndex,
	cfg AvailabilityGossipConfig,
) *AvailabilityGossip {
	cfg.withDefaults()
	g := &AvailabilityGossip{
		mesh:   mesh,
		local:  local,
		remote: remote,
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}

	// Register delta callback to broadcast changes.
	local.OnChange(g.broadcastDelta)

	return g
}

// Start begins the periodic full-sync loop.
func (g *AvailabilityGossip) Start() {
	go g.syncLoop()
}

// Stop shuts down the gossip coordinator.
func (g *AvailabilityGossip) Stop() {
	close(g.stopCh)
	<-g.done
}

// OnPeerJoined should be called when a new peer connects. Sends a full
// snapshot to the new peer.
func (g *AvailabilityGossip) OnPeerJoined(peerID string) {
	snapData, err := g.local.Snapshot()
	if err != nil {
		return
	}
	data := EncodeSnapshot(snapData)
	g.mesh.SendStream(peerID, data)
}

// OnPeerLeft should be called when a peer disconnects. Clears their
// availability.
func (g *AvailabilityGossip) OnPeerLeft(peerID string) {
	g.remote.RemovePeer(peerID)
}

// HandleSnapshot processes a received snapshot from a remote peer.
// Expects post-dispatch data starting from the sub-type byte.
func (g *AvailabilityGossip) HandleSnapshot(peerID string, data []byte) error {
	bitmapData, err := DecodeSnapshot(data)
	if err != nil {
		return err
	}
	return g.remote.ApplySnapshot(peerID, bitmapData)
}

// HandleDelta processes a received delta from a remote peer.
// Expects post-dispatch data starting from the sub-type byte.
func (g *AvailabilityGossip) HandleDelta(peerID string, data []byte) error {
	op, pageNo, err := DecodeDelta(data)
	if err != nil {
		return err
	}
	g.remote.ApplyDelta(peerID, op, pageNo)
	return nil
}

// HandleMessage dispatches a received post-dispatch message based on the
// sub-type byte. The transport layer has already stripped the
// StreamTypeAvailability prefix during dispatch.
func (g *AvailabilityGossip) HandleMessage(peerID string, data []byte) error {
	if len(data) < 1 {
		return nil
	}
	switch data[0] {
	case AvailSubSnapshot:
		return g.HandleSnapshot(peerID, data)
	case AvailSubDelta:
		return g.HandleDelta(peerID, data)
	}
	return nil
}

// broadcastDelta sends a delta to all live peers via unreliable datagram.
// Runs the broadcast loop in a goroutine to avoid blocking the caller
// (which may hold locks).
func (g *AvailabilityGossip) broadcastDelta(op DeltaOp, pageNo uint32) {
	data := EncodeDelta(op, pageNo)
	go func() {
		for _, peerID := range g.mesh.LivePeers() {
			g.mesh.SendDatagram(peerID, data)
		}
	}()
}

// syncLoop periodically sends full snapshots to all peers.
func (g *AvailabilityGossip) syncLoop() {
	defer close(g.done)
	ticker := time.NewTicker(g.cfg.FullSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.broadcastSnapshot()
		}
	}
}

// broadcastSnapshot sends a full snapshot to all live peers via reliable stream.
func (g *AvailabilityGossip) broadcastSnapshot() {
	snapData, err := g.local.Snapshot()
	if err != nil {
		return
	}
	data := EncodeSnapshot(snapData)
	for _, peerID := range g.mesh.LivePeers() {
		g.mesh.SendStream(peerID, data)
	}
}
