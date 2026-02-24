package prefetch

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// DeltaOp indicates whether an availability delta adds or removes pages.
type DeltaOp byte

const (
	DeltaAdd    DeltaOp = 0x01
	DeltaRemove DeltaOp = 0x02
)

// AvailabilityIndex stores remote peer availability as roaring64 bitmaps
// over physical page numbers.
type AvailabilityIndex struct {
	mu    sync.Mutex
	peers map[string]*roaring64.Bitmap
}

// NewAvailabilityIndex creates an AvailabilityIndex.
func NewAvailabilityIndex() *AvailabilityIndex {
	return &AvailabilityIndex{
		peers: make(map[string]*roaring64.Bitmap),
	}
}

// HasPage returns true if the peer is known to have the given page.
func (a *AvailabilityIndex) HasPage(peerID string, pageNo uint32) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	bm, ok := a.peers[peerID]
	if !ok {
		return false
	}
	return bm.Contains(uint64(pageNo))
}

// ApplySnapshot replaces a peer's full availability with the given
// serialized roaring64 bitmap.
func (a *AvailabilityIndex) ApplySnapshot(peerID string, data []byte) error {
	bm := roaring64.New()
	if len(data) > 0 {
		if err := bm.UnmarshalBinary(data); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.peers[peerID] = bm
	return nil
}

// ApplyDelta adds or removes a single page for a peer.
func (a *AvailabilityIndex) ApplyDelta(peerID string, op DeltaOp, pageNo uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	bm, ok := a.peers[peerID]
	if !ok {
		bm = roaring64.New()
		a.peers[peerID] = bm
	}
	switch op {
	case DeltaAdd:
		bm.Add(uint64(pageNo))
	case DeltaRemove:
		bm.Remove(uint64(pageNo))
	}
}

// RemovePeer removes all availability state for a peer.
func (a *AvailabilityIndex) RemovePeer(peerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.peers, peerID)
}

// Reset clears all availability state.
func (a *AvailabilityIndex) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.peers = make(map[string]*roaring64.Bitmap)
}

// LocalAvailability tracks which pages the local node has cached as a
// roaring64 bitmap over physical page numbers. Fires onChange callbacks
// for gossip broadcast.
type LocalAvailability struct {
	mu       sync.Mutex
	bitmap   *roaring64.Bitmap
	onChange []func(DeltaOp, uint32)
}

// NewLocalAvailability creates a LocalAvailability tracker.
func NewLocalAvailability() *LocalAvailability {
	return &LocalAvailability{
		bitmap: roaring64.New(),
	}
}

// OnChange registers a callback that fires when local availability changes.
func (l *LocalAvailability) OnChange(fn func(DeltaOp, uint32)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = append(l.onChange, fn)
}

// OnPageCached records that a page has been cached locally.
func (l *LocalAvailability) OnPageCached(pageNo uint32) {
	l.mu.Lock()
	addr := uint64(pageNo)
	if l.bitmap.Contains(addr) {
		l.mu.Unlock()
		return // already tracked
	}
	l.bitmap.Add(addr)
	callbacks := make([]func(DeltaOp, uint32), len(l.onChange))
	copy(callbacks, l.onChange)
	l.mu.Unlock()

	for _, fn := range callbacks {
		fn(DeltaAdd, pageNo)
	}
}

// Snapshot returns the serialized roaring64 bitmap of local availability.
func (l *LocalAvailability) Snapshot() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bitmap.RunOptimize()
	return l.bitmap.MarshalBinary()
}

// Reset clears all local availability state.
func (l *LocalAvailability) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bitmap = roaring64.New()
}
