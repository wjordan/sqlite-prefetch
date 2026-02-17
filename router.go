package prefetch

import (
	"math/rand"
	"sync"
)

const (
	defaultExploreRate = 0.10
	defaultMaxHints    = 32768
	ewmaAlpha          = 0.3
	minHitRateForRoute = 0.3 // peer must exceed this hit rate to be routed to without a hint
)

// peerScore tracks a peer's hit rate via EWMA.
type peerScore struct {
	hitRate float64
	samples int
}

// pageHint records which peer is predicted to have a page.
type pageHint struct {
	peerID string
}

// PeerRouter learns peer content from fetch hit/miss outcomes and uses
// B-tree sibling amplification to predict peer content after a single hit.
// All intelligence is requester-side — no changes to the wire protocol.
type PeerRouter struct {
	mu        sync.Mutex
	peers     map[string]*peerScore // peerID -> EWMA hit rate
	pageHints map[uint32]pageHint   // pageNo -> predicted peer
	leader    string                // slot leader peerID — preferred for unhinted pages
	readahead *ReadaheadEngine      // for Siblings queries (nil-safe)
	rng       *rand.Rand
	explore   float64
	maxHints  int
}

// NewPeerRouter creates a PeerRouter. readahead may be nil (no sibling expansion).
func NewPeerRouter(readahead *ReadaheadEngine) *PeerRouter {
	return &PeerRouter{
		peers:     make(map[string]*peerScore),
		pageHints: make(map[uint32]pageHint),
		readahead: readahead,
		rng:       rand.New(rand.NewSource(1)),
		explore:   defaultExploreRate,
		maxHints:  defaultMaxHints,
	}
}

// RecordResult updates peer score and page hints based on a fetch outcome.
// On hit: records positive score, expands sibling hints via btreeTracker.
// On miss: records negative score, removes stale hint for this page.
func (r *PeerRouter) RecordResult(peerID string, pageNo int64, hit bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ps := r.getOrCreatePeer(peerID)

	if hit {
		// Update EWMA toward 1.0.
		if ps.samples == 0 {
			ps.hitRate = 1.0
		} else {
			ps.hitRate = ewmaAlpha*1.0 + (1-ewmaAlpha)*ps.hitRate
		}
		ps.samples++

		// Set page hint.
		r.setHint(uint32(pageNo), peerID)

		// Expand to siblings via readahead's btreeTracker.
		if r.readahead != nil {
			siblings := r.readahead.Siblings(uint32(pageNo))
			for _, sib := range siblings {
				r.setHint(sib, peerID)
			}
		}
	} else {
		// Update EWMA toward 0.0.
		if ps.samples == 0 {
			ps.hitRate = 0.0
		} else {
			ps.hitRate = ewmaAlpha*0.0 + (1-ewmaAlpha)*ps.hitRate
		}
		ps.samples++

		// Remove stale hint if it pointed to this peer.
		if hint, ok := r.pageHints[uint32(pageNo)]; ok && hint.peerID == peerID {
			delete(r.pageHints, uint32(pageNo))
		}
	}
}

// ShouldTry returns true if the given peer should be tried for the page.
// Decision order: page hint > leader preference > good hit rate > exploration.
func (r *PeerRouter) ShouldTry(peerID string, pageNo int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Page hint match.
	if hint, ok := r.pageHints[uint32(pageNo)]; ok {
		return hint.peerID == peerID
	}

	// 2. Leader preference — the slot leader has all pages in cache.
	if r.leader != "" {
		return peerID == r.leader
	}

	// 3. Good hit rate.
	if ps, ok := r.peers[peerID]; ok && ps.samples > 0 && ps.hitRate > minHitRateForRoute {
		return true
	}

	// 4. Exploration.
	return r.rng.Float64() < r.explore
}

// SetLeader sets the slot leader peer. The leader always has all pages
// in cache (it just wrote them), so unhinted pages are routed there
// directly — eliminating cold-start exploration.
func (r *PeerRouter) SetLeader(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leader = peerID
}

// Leader returns the current leader peer ID (for testing).
func (r *PeerRouter) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

// RemovePeer removes all state for a peer.
func (r *PeerRouter) RemovePeer(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.peers, peerID)
	if r.leader == peerID {
		r.leader = ""
	}
	for pgno, hint := range r.pageHints {
		if hint.peerID == peerID {
			delete(r.pageHints, pgno)
		}
	}
}

// RetainOnly drops hints and scores for peers not in the given set.
func (r *PeerRouter) RetainOnly(peerIDs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	keep := make(map[string]bool, len(peerIDs))
	for _, id := range peerIDs {
		keep[id] = true
	}

	for id := range r.peers {
		if !keep[id] {
			delete(r.peers, id)
		}
	}
	if r.leader != "" && !keep[r.leader] {
		r.leader = ""
	}
	for pgno, hint := range r.pageHints {
		if !keep[hint.peerID] {
			delete(r.pageHints, pgno)
		}
	}
}

// Reset clears page hints but keeps peer scores. Called on rebase when
// page layout may have changed.
func (r *PeerRouter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pageHints = make(map[uint32]pageHint)
}

// HintCount returns the number of page hints (for testing).
func (r *PeerRouter) HintCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pageHints)
}

// PeerHitRate returns the EWMA hit rate for a peer (for testing).
func (r *PeerRouter) PeerHitRate(peerID string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ps, ok := r.peers[peerID]; ok {
		return ps.hitRate
	}
	return 0
}

// getOrCreatePeer returns the score for a peer, creating it if needed.
// Caller must hold r.mu.
func (r *PeerRouter) getOrCreatePeer(peerID string) *peerScore {
	ps, ok := r.peers[peerID]
	if !ok {
		ps = &peerScore{}
		r.peers[peerID] = ps
	}
	return ps
}

// setHint sets a page hint, enforcing the maxHints bound.
// Caller must hold r.mu.
func (r *PeerRouter) setHint(pageNo uint32, peerID string) {
	if len(r.pageHints) >= r.maxHints {
		// Already at capacity — only overwrite existing hints, don't add new ones.
		if _, exists := r.pageHints[pageNo]; !exists {
			return
		}
	}
	r.pageHints[pageNo] = pageHint{peerID: peerID}
}
