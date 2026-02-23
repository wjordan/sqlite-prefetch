package prefetch

import (
	"slices"
	"sync"
)

// ChildExtent represents a contiguous range of children within an interior
// page's child array. Start is 0-based index, Count is the number of
// consecutive children.
type ChildExtent struct {
	Start uint16
	Count uint16
}

// DeltaOp indicates whether an availability delta adds or removes extents.
type DeltaOp byte

const (
	DeltaAdd    DeltaOp = 0x01
	DeltaRemove DeltaOp = 0x02
)

// AvailabilityDelta describes a change to a peer's availability for one
// interior page.
type AvailabilityDelta struct {
	Op           DeltaOp
	InteriorPage uint32
	Extents      []ChildExtent
}

// PageAvailability describes which children of an interior page are available.
type PageAvailability struct {
	InteriorPage uint32
	Extents      []ChildExtent
}

// ChildLookup resolves interior page numbers to their child page numbers.
// This decouples AvailabilityIndex from sqlitebtree.Tracker.
type ChildLookup interface {
	Children(interiorPage uint32) ([]uint32, bool)
}

// AvailabilityIndex stores remote peer availability using child-index extents.
// Two-level structure:
//   - Opaque layer: extents stored immediately from gossip before interior page
//     is parsed locally
//   - Resolved layer: built lazily when the local node parses the interior page
type AvailabilityIndex struct {
	mu sync.Mutex

	// extents[peerID][interiorPage] → []ChildExtent
	extents map[string]map[uint32][]ChildExtent

	// resolved[childPageNo] → set of peerIDs
	resolved map[uint32]map[string]struct{}

	// parsedInteriors tracks which interior pages have been parsed locally,
	// so we know when to resolve opaque extents.
	parsedInteriors map[uint32]bool

	lookup ChildLookup
}

// NewAvailabilityIndex creates an AvailabilityIndex.
func NewAvailabilityIndex(lookup ChildLookup) *AvailabilityIndex {
	return &AvailabilityIndex{
		extents:         make(map[string]map[uint32][]ChildExtent),
		resolved:        make(map[uint32]map[string]struct{}),
		parsedInteriors: make(map[uint32]bool),
		lookup:          lookup,
	}
}

// HasPage returns true if the peer is known to have the given page.
// Only checks the resolved layer — if the interior page hasn't been parsed
// locally yet, returns false (correct: children aren't needed before their
// parent is fetched).
func (a *AvailabilityIndex) HasPage(peerID string, pageNo uint32) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	peers, ok := a.resolved[pageNo]
	if !ok {
		return false
	}
	_, found := peers[peerID]
	return found
}

// ApplyDelta applies an availability delta from a remote peer.
func (a *AvailabilityIndex) ApplyDelta(peerID string, delta AvailabilityDelta) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch delta.Op {
	case DeltaAdd:
		a.addExtents(peerID, delta.InteriorPage, delta.Extents)
	case DeltaRemove:
		a.removeExtents(peerID, delta.InteriorPage, delta.Extents)
	}
}

// ApplySnapshot replaces a peer's full availability with the given snapshot.
func (a *AvailabilityIndex) ApplySnapshot(peerID string, pages []PageAvailability) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Remove all existing resolved entries for this peer.
	a.removePeerResolved(peerID)

	// Replace extents.
	peerExtents := make(map[uint32][]ChildExtent, len(pages))
	for _, pa := range pages {
		peerExtents[pa.InteriorPage] = pa.Extents
	}
	a.extents[peerID] = peerExtents

	// Resolve any extents for already-parsed interior pages.
	for _, pa := range pages {
		if a.parsedInteriors[pa.InteriorPage] {
			a.resolveExtents(peerID, pa.InteriorPage, pa.Extents)
		}
	}
}

// RemovePeer removes all availability state for a peer.
func (a *AvailabilityIndex) RemovePeer(peerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.removePeerResolved(peerID)
	delete(a.extents, peerID)
}

// OnInteriorPageParsed is called when the local node fetches and parses an
// interior page. This triggers resolution of all opaque extents for that page
// across all peers.
func (a *AvailabilityIndex) OnInteriorPageParsed(interiorPage uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.parsedInteriors[interiorPage] = true

	// Resolve extents from all peers for this interior page.
	for peerID, peerExtents := range a.extents {
		if exts, ok := peerExtents[interiorPage]; ok {
			a.resolveExtents(peerID, interiorPage, exts)
		}
	}
}

// Reset clears all availability state. Called on rebase when page layout may
// have changed.
func (a *AvailabilityIndex) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.extents = make(map[string]map[uint32][]ChildExtent)
	a.resolved = make(map[uint32]map[string]struct{})
	a.parsedInteriors = make(map[uint32]bool)
}

// addExtents stores extents for a peer and resolves them if the interior page
// is already parsed. Caller must hold a.mu.
func (a *AvailabilityIndex) addExtents(peerID string, interiorPage uint32, extents []ChildExtent) {
	peerExtents, ok := a.extents[peerID]
	if !ok {
		peerExtents = make(map[uint32][]ChildExtent)
		a.extents[peerID] = peerExtents
	}
	peerExtents[interiorPage] = extents

	if a.parsedInteriors[interiorPage] {
		a.resolveExtents(peerID, interiorPage, extents)
	}
}

// removeExtents removes specific extents for a peer and unresolves them.
// Caller must hold a.mu.
func (a *AvailabilityIndex) removeExtents(peerID string, interiorPage uint32, extents []ChildExtent) {
	// Unresolve the child pages for these extents.
	if a.parsedInteriors[interiorPage] {
		a.forEachChild(interiorPage, extents, func(childPage uint32) {
			if peers, exists := a.resolved[childPage]; exists {
				delete(peers, peerID)
				if len(peers) == 0 {
					delete(a.resolved, childPage)
				}
			}
		})
	}

	// Remove from opaque extents.
	if peerExtents, ok := a.extents[peerID]; ok {
		delete(peerExtents, interiorPage)
		if len(peerExtents) == 0 {
			delete(a.extents, peerID)
		}
	}
}

// resolveExtents maps child-index extents to physical page numbers and adds
// them to the resolved layer. Caller must hold a.mu.
func (a *AvailabilityIndex) resolveExtents(peerID string, interiorPage uint32, extents []ChildExtent) {
	a.forEachChild(interiorPage, extents, func(childPage uint32) {
		peers, exists := a.resolved[childPage]
		if !exists {
			peers = make(map[string]struct{})
			a.resolved[childPage] = peers
		}
		peers[peerID] = struct{}{}
	})
}

// forEachChild iterates over child page numbers covered by the given extents
// for an interior page. Caller must hold a.mu.
func (a *AvailabilityIndex) forEachChild(interiorPage uint32, extents []ChildExtent, fn func(childPage uint32)) {
	children, ok := a.lookup.Children(interiorPage)
	if !ok {
		return
	}
	for _, ext := range extents {
		start := int(ext.Start)
		end := start + int(ext.Count)
		if end > len(children) {
			end = len(children)
		}
		for i := start; i < end; i++ {
			fn(children[i])
		}
	}
}

// removePeerResolved removes all resolved entries for a peer.
// Caller must hold a.mu.
func (a *AvailabilityIndex) removePeerResolved(peerID string) {
	for pgno, peers := range a.resolved {
		delete(peers, peerID)
		if len(peers) == 0 {
			delete(a.resolved, pgno)
		}
	}
}

// LocalAvailability tracks which pages the local node has cached, expressed
// as child-index extents. Fires onChange callbacks for gossip broadcast.
type LocalAvailability struct {
	mu sync.Mutex

	// cachedPages tracks which child indices are cached per interior page.
	// cachedPages[interiorPage][childIdx] = true
	cachedPages map[uint32]map[uint16]bool

	// childToInterior maps child page number → (interiorPage, childIdx).
	childToInterior map[uint32]interiorPosition

	// parsedInteriors[interiorPage] = childCount
	parsedInteriors map[uint32]int

	lookup    ChildLookup
	onChange  []func(AvailabilityDelta)
}

// interiorPosition records a page's position in its parent interior page.
type interiorPosition struct {
	interiorPage uint32
	childIdx     uint16
}

// NewLocalAvailability creates a LocalAvailability tracker.
func NewLocalAvailability(lookup ChildLookup) *LocalAvailability {
	return &LocalAvailability{
		cachedPages:     make(map[uint32]map[uint16]bool),
		childToInterior: make(map[uint32]interiorPosition),
		parsedInteriors: make(map[uint32]int),
		lookup:          lookup,
	}
}

// OnChange registers a callback that fires when local availability changes.
func (l *LocalAvailability) OnChange(fn func(AvailabilityDelta)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = append(l.onChange, fn)
}

// OnPageCached records that a page has been cached locally.
func (l *LocalAvailability) OnPageCached(pageNo uint32) {
	l.mu.Lock()

	pos, ok := l.childToInterior[pageNo]
	if !ok {
		l.mu.Unlock()
		return
	}

	pages, exists := l.cachedPages[pos.interiorPage]
	if !exists {
		pages = make(map[uint16]bool)
		l.cachedPages[pos.interiorPage] = pages
	}
	if pages[pos.childIdx] {
		l.mu.Unlock()
		return // already tracked
	}
	pages[pos.childIdx] = true

	// Build delta with current extents for this interior page.
	extents := l.buildExtents(pos.interiorPage)
	delta := AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: pos.interiorPage,
		Extents:      extents,
	}
	callbacks := make([]func(AvailabilityDelta), len(l.onChange))
	copy(callbacks, l.onChange)
	l.mu.Unlock()

	for _, fn := range callbacks {
		fn(delta)
	}
}

// OnInteriorPageParsed is called when an interior page is fetched and parsed.
// This establishes the child→interior mapping for all children of that page.
func (l *LocalAvailability) OnInteriorPageParsed(interiorPage uint32, children []uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.parsedInteriors[interiorPage] = len(children)
	for i, child := range children {
		l.childToInterior[child] = interiorPosition{
			interiorPage: interiorPage,
			childIdx:     uint16(i),
		}
	}
}

// Snapshot returns the full local availability as a list of PageAvailability.
func (l *LocalAvailability) Snapshot() []PageAvailability {
	l.mu.Lock()
	defer l.mu.Unlock()

	var result []PageAvailability
	for interiorPage, pages := range l.cachedPages {
		if len(pages) == 0 {
			continue
		}
		extents := l.buildExtents(interiorPage)
		result = append(result, PageAvailability{
			InteriorPage: interiorPage,
			Extents:      extents,
		})
	}
	return result
}

// Reset clears all local availability state.
func (l *LocalAvailability) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.cachedPages = make(map[uint32]map[uint16]bool)
	l.childToInterior = make(map[uint32]interiorPosition)
	l.parsedInteriors = make(map[uint32]int)
}

// buildExtents computes compacted extents from the cached child indices for
// an interior page. Caller must hold l.mu.
func (l *LocalAvailability) buildExtents(interiorPage uint32) []ChildExtent {
	pages := l.cachedPages[interiorPage]
	if len(pages) == 0 {
		return nil
	}

	// Collect and sort indices.
	childCount := l.parsedInteriors[interiorPage]
	if childCount == 0 {
		return nil
	}

	// Build sorted list of cached indices.
	sorted := make([]uint16, 0, len(pages))
	for idx := range pages {
		sorted = append(sorted, idx)
	}
	slices.Sort(sorted)

	// Compact into contiguous extents.
	var extents []ChildExtent
	start := sorted[0]
	count := uint16(1)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == start+count {
			count++
		} else {
			extents = append(extents, ChildExtent{Start: start, Count: count})
			start = sorted[i]
			count = 1
		}
	}
	extents = append(extents, ChildExtent{Start: start, Count: count})
	return extents
}
