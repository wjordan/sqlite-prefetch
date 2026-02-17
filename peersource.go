package prefetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// PeerTransport opens bidirectional streams to peers for page fetching.
// Implementations typically wrap QUIC, TCP, or other transport protocols.
type PeerTransport interface {
	OpenStream(ctx context.Context, addr string) (io.ReadWriteCloser, error)
}

// StreamTypePageFetch is the wire protocol prefix byte identifying page fetch
// streams. The requester writes this as the first byte so the responder can
// dispatch to the correct handler.
const StreamTypePageFetch byte = 0x12

const (
	peerStatusHit  byte = 0x01
	peerStatusMiss byte = 0x00
)

// PeerSourceConfig controls the PeerSource behaviour and defaults.
type PeerSourceConfig struct {
	FetchTimeout     time.Duration // Default: 2s
	DefaultLatency   time.Duration // Default: 5ms
	DefaultBandwidth float64       // Default: 100MB/s
	EWMAAlpha        float64       // Default: 0.3
}

func (c *PeerSourceConfig) withDefaults() {
	if c.FetchTimeout <= 0 {
		c.FetchTimeout = 2 * time.Second
	}
	if c.DefaultLatency <= 0 {
		c.DefaultLatency = 5 * time.Millisecond
	}
	if c.DefaultBandwidth <= 0 {
		c.DefaultBandwidth = 100 * 1024 * 1024
	}
	if c.EWMAAlpha <= 0 || c.EWMAAlpha > 1 {
		c.EWMAAlpha = 0.3
	}
}

// PeerSource fetches pages from a single peer via a bidirectional stream.
// It implements the Source interface, tracking latency and bandwidth
// via EWMA. Peers have zero egress cost and partial completeness
// (pages available depend on the peer's cache).
type PeerSource struct {
	transport PeerTransport
	nodeID    string
	addr      string // peer's transport address
	router    *PeerRouter
	cfg       PeerSourceConfig
	latency   *ewma
	bandwidth *ewma
}

// Compile-time check that PeerSource satisfies Source.
var _ Source = (*PeerSource)(nil)

// NewPeerSource creates a PeerSource for the given peer.
func NewPeerSource(t PeerTransport, nodeID, addr string, router *PeerRouter, cfg PeerSourceConfig) *PeerSource {
	cfg.withDefaults()
	lat := newEWMA(cfg.EWMAAlpha)
	lat.Update(float64(cfg.DefaultLatency))
	bw := newEWMA(cfg.EWMAAlpha)
	bw.Update(cfg.DefaultBandwidth)
	return &PeerSource{
		transport: t,
		nodeID:    nodeID,
		addr:      addr,
		router:    router,
		cfg:       cfg,
		latency:   lat,
		bandwidth: bw,
	}
}

func (p *PeerSource) Name() string           { return "peer:" + p.nodeID }
func (p *PeerSource) Latency() time.Duration { return time.Duration(p.latency.Value()) }
func (p *PeerSource) Bandwidth() float64     { return p.bandwidth.Value() }
func (p *PeerSource) Completeness() float64  { return 0.5 }
func (p *PeerSource) EgressCost() float64    { return 0 }

// HasPage checks the router for whether this peer should be tried for the page.
// Returns false if no router is set.
func (p *PeerSource) HasPage(pageNo int64) bool {
	if p.router == nil {
		return false
	}
	return p.router.ShouldTry(p.nodeID, pageNo)
}

// NodeID returns the peer's node identifier.
func (p *PeerSource) NodeID() string { return p.nodeID }

// Addr returns the peer's transport address.
func (p *PeerSource) Addr() string { return p.addr }

// GetPage fetches a page from the peer via a bidirectional stream,
// measuring latency and bandwidth to update the EWMA trackers.
// Records hit/miss results in the router for learning.
func (p *PeerSource) GetPage(ctx context.Context, pageNo int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.FetchTimeout)
	defer cancel()

	s, err := p.transport.OpenStream(ctx, p.addr)
	if err != nil {
		return nil, fmt.Errorf("peer %s: open stream: %w", p.nodeID, err)
	}
	defer s.Close()

	// Write: [StreamTypePageFetch][4B pageNo BE]
	var req [5]byte
	req[0] = StreamTypePageFetch
	binary.BigEndian.PutUint32(req[1:], uint32(pageNo))

	start := time.Now()
	if _, err := s.Write(req[:]); err != nil {
		return nil, fmt.Errorf("peer %s: write request: %w", p.nodeID, err)
	}

	// Read: [1B status][page data if hit]
	var status [1]byte
	if _, err := io.ReadFull(s, status[:]); err != nil {
		return nil, fmt.Errorf("peer %s: read status: %w", p.nodeID, err)
	}
	elapsed := time.Since(start)

	if status[0] == peerStatusMiss {
		if p.router != nil {
			p.router.RecordResult(p.nodeID, pageNo, false)
		}
		return nil, fmt.Errorf("peer %s: miss for page %d", p.nodeID, pageNo)
	}
	if status[0] != peerStatusHit {
		return nil, fmt.Errorf("peer %s: unknown status %d", p.nodeID, status[0])
	}

	data, err := io.ReadAll(s)
	if err != nil {
		return nil, fmt.Errorf("peer %s: read page data: %w", p.nodeID, err)
	}

	p.latency.Update(float64(elapsed))
	if elapsed > 0 {
		p.bandwidth.Update(float64(len(data)) / elapsed.Seconds())
	}
	if p.router != nil {
		p.router.RecordResult(p.nodeID, pageNo, true)
	}
	return data, nil
}
