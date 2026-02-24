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

// LogicalStride is the multiplier for converting an interior page number to
// the base of its logical address range. Each interior page occupies a
// contiguous block of LogicalStride addresses, one per child.
const LogicalStride uint64 = 4096

// LogicalAddressMap maps physical page numbers to logical addresses for the
// availability bitmap. Logical addresses are computed as:
//
//	uint64(interiorPageNo) * LogicalStride + uint64(childIdx)
//
// This mapping is populated as interior pages are parsed and is shared
// between LocalAvailability and AvailabilityIndex.
type LogicalAddressMap struct {
	mu            sync.RWMutex
	physToLogical map[uint32]uint64
}

// NewLogicalAddressMap creates an empty LogicalAddressMap.
func NewLogicalAddressMap() *LogicalAddressMap {
	return &LogicalAddressMap{
		physToLogical: make(map[uint32]uint64),
	}
}

// Register records the logical addresses for all children of an interior page.
func (m *LogicalAddressMap) Register(interiorPage uint32, childPages []uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	base := uint64(interiorPage) * LogicalStride
	for i, child := range childPages {
		m.physToLogical[child] = base + uint64(i)
	}
}

// Lookup returns the logical address for a physical page number.
func (m *LogicalAddressMap) Lookup(physicalPage uint32) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addr, ok := m.physToLogical[physicalPage]
	return addr, ok
}

// Reset clears all mappings.
func (m *LogicalAddressMap) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.physToLogical = make(map[uint32]uint64)
}

// AvailabilityIndex stores remote peer availability as roaring64 bitmaps
// over logical addresses.
type AvailabilityIndex struct {
	mu      sync.Mutex
	addrMap *LogicalAddressMap
	peers   map[string]*roaring64.Bitmap
}

// NewAvailabilityIndex creates an AvailabilityIndex backed by the given
// logical address map.
func NewAvailabilityIndex(addrMap *LogicalAddressMap) *AvailabilityIndex {
	return &AvailabilityIndex{
		addrMap: addrMap,
		peers:   make(map[string]*roaring64.Bitmap),
	}
}

// HasPage returns true if the peer is known to have the given page.
// Returns false if the page has no logical address mapping (interior page
// not yet parsed).
func (a *AvailabilityIndex) HasPage(peerID string, pageNo uint32) bool {
	logicalAddr, ok := a.addrMap.Lookup(pageNo)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	bm, ok := a.peers[peerID]
	if !ok {
		return false
	}
	return bm.Contains(logicalAddr)
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

// ApplyDelta adds or removes a single logical address for a peer.
func (a *AvailabilityIndex) ApplyDelta(peerID string, op DeltaOp, logicalAddr uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	bm, ok := a.peers[peerID]
	if !ok {
		bm = roaring64.New()
		a.peers[peerID] = bm
	}
	switch op {
	case DeltaAdd:
		bm.Add(logicalAddr)
	case DeltaRemove:
		bm.Remove(logicalAddr)
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
// roaring64 bitmap over logical addresses. Fires onChange callbacks for
// gossip broadcast.
type LocalAvailability struct {
	mu       sync.Mutex
	addrMap  *LogicalAddressMap
	bitmap   *roaring64.Bitmap
	onChange []func(DeltaOp, uint64)
}

// NewLocalAvailability creates a LocalAvailability tracker.
func NewLocalAvailability(addrMap *LogicalAddressMap) *LocalAvailability {
	return &LocalAvailability{
		addrMap: addrMap,
		bitmap:  roaring64.New(),
	}
}

// OnChange registers a callback that fires when local availability changes.
func (l *LocalAvailability) OnChange(fn func(DeltaOp, uint64)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = append(l.onChange, fn)
}

// OnPageCached records that a page has been cached locally.
func (l *LocalAvailability) OnPageCached(pageNo uint32) {
	logicalAddr, ok := l.addrMap.Lookup(pageNo)
	if !ok {
		return
	}

	l.mu.Lock()
	if l.bitmap.Contains(logicalAddr) {
		l.mu.Unlock()
		return // already tracked
	}
	l.bitmap.Add(logicalAddr)
	callbacks := make([]func(DeltaOp, uint64), len(l.onChange))
	copy(callbacks, l.onChange)
	l.mu.Unlock()

	for _, fn := range callbacks {
		fn(DeltaAdd, logicalAddr)
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
