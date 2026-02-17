package prefetch

import (
	"encoding/binary"
	"fmt"
)

// parseInteriorTablePage parses a SQLite interior table B-tree page (flag 0x05)
// and returns child page numbers in left-to-right traversal order. Page 1 has a
// 100-byte database header before the B-tree header; pass pageNo=1 to handle it.
func parseInteriorTablePage(data []byte, pageNo uint32) ([]uint32, error) {
	// Page 1 has a 100-byte SQLite database header.
	hdrOff := 0
	if pageNo == 1 {
		hdrOff = 100
	}

	// Interior page header is 12 bytes.
	if len(data) < hdrOff+12 {
		return nil, fmt.Errorf("btree: page too small (%d bytes)", len(data))
	}

	flag := data[hdrOff]
	if flag != 0x05 {
		return nil, fmt.Errorf("btree: not an interior table page (flag=0x%02x)", flag)
	}

	cellCount := int(binary.BigEndian.Uint16(data[hdrOff+3 : hdrOff+5]))
	if cellCount == 0 {
		return nil, fmt.Errorf("btree: zero cells")
	}

	rightChild := binary.BigEndian.Uint32(data[hdrOff+8 : hdrOff+12])

	// Cell pointer array starts at hdrOff+12, each entry is 2 bytes.
	ptrArrayEnd := hdrOff + 12 + cellCount*2
	if ptrArrayEnd > len(data) {
		return nil, fmt.Errorf("btree: cell pointer array overflows page")
	}

	children := make([]uint32, 0, cellCount+1)
	for i := 0; i < cellCount; i++ {
		cellOff := int(binary.BigEndian.Uint16(data[hdrOff+12+i*2 : hdrOff+14+i*2]))
		// Each cell needs at least 4 bytes for the child page number.
		if cellOff+4 > len(data) {
			return nil, fmt.Errorf("btree: cell %d offset %d out of bounds", i, cellOff)
		}
		child := binary.BigEndian.Uint32(data[cellOff : cellOff+4])
		children = append(children, child)
	}

	// Rightmost child is appended last (right edge of the B-tree level).
	children = append(children, rightChild)
	return children, nil
}

// btreeTracker learns B-tree structure from fetched pages and predicts
// upcoming leaf page accesses. Not thread-safe — caller must synchronize
// (ReadaheadEngine holds its mutex during OnFault/OnFetchComplete calls).
type btreeTracker struct {
	interiorChildren map[uint32][]uint32 // parent pgno → ordered child list
	childToParent    map[uint32]uint32   // child pgno → parent pgno
	maxTracked       int                 // max interior pages to track
}

func newBtreeTracker(maxTracked int) *btreeTracker {
	return &btreeTracker{
		interiorChildren: make(map[uint32][]uint32),
		childToParent:    make(map[uint32]uint32),
		maxTracked:       maxTracked,
	}
}

// OnFetchComplete inspects fetched page data. If it's an interior table page,
// parses the child pointers and stores the mapping. pageNo is 0-based
// (Prefetcher convention); child pointers in the B-tree are 1-based SQLite
// pgno values. This method converts children to 0-based before storing.
func (bt *btreeTracker) OnFetchComplete(pageNo uint32, data []byte) {
	// Convert 0-based pageNo to 1-based SQLite pgno for the parser.
	children, err := parseInteriorTablePage(data, pageNo+1)
	if err != nil {
		return // not an interior page or malformed — silently ignore
	}

	// Evict if over limit.
	for len(bt.interiorChildren) >= bt.maxTracked {
		bt.evictOne()
	}

	// Convert 1-based SQLite pgno children to 0-based Prefetcher pageNo.
	converted := make([]uint32, len(children))
	for i, child := range children {
		converted[i] = child - 1
	}

	bt.interiorChildren[pageNo] = converted
	for _, child := range converted {
		bt.childToParent[child] = pageNo
	}
}

const lookaheadThreshold = 10

// Predict returns the remaining sibling pages after pageNo in its parent's
// child list. When remaining siblings <= lookaheadThreshold, also appends the
// next interior sibling page (multi-level lookahead) so it gets prefetched
// and parsed before SQLite descends into it.
// PredictResult gives the reason Predict returned false.
type PredictResult int

const (
	PredictOK        PredictResult = iota // predicted siblings
	PredictNotChild                       // page not in childToParent
	PredictLastChild                      // page is the last child (no remaining siblings)
)

func (bt *btreeTracker) Predict(pageNo uint32) ([]uint32, PredictResult) {
	parent, ok := bt.childToParent[pageNo]
	if !ok {
		return nil, PredictNotChild
	}
	children := bt.interiorChildren[parent]

	// Find position of pageNo in children.
	idx := -1
	for i, c := range children {
		if c == pageNo {
			idx = i
			break
		}
	}
	if idx < 0 || idx >= len(children)-1 {
		return nil, PredictLastChild
	}

	remaining := children[idx+1:]
	result := make([]uint32, len(remaining))
	copy(result, remaining)

	// Multi-level lookahead: when nearing exhaustion of this interior page's
	// children, prefetch the next sibling interior page so its children are
	// parsed and ready before SQLite descends into it.
	if len(remaining) <= lookaheadThreshold {
		if nextInterior, ok := bt.nextInteriorSibling(parent); ok {
			result = append(result, nextInterior)
		}
	}

	return result, PredictOK
}

// nextInteriorSibling finds the next sibling of interiorPgNo in its parent's
// child list.
func (bt *btreeTracker) nextInteriorSibling(interiorPgNo uint32) (uint32, bool) {
	grandparent, ok := bt.childToParent[interiorPgNo]
	if !ok {
		return 0, false
	}
	gpChildren := bt.interiorChildren[grandparent]
	for i, c := range gpChildren {
		if c == interiorPgNo && i < len(gpChildren)-1 {
			return gpChildren[i+1], true
		}
	}
	return 0, false
}

// Siblings returns all children of pageNo's parent (the full sibling set),
// including pageNo itself. Returns nil if pageNo is not a known child.
func (bt *btreeTracker) Siblings(pageNo uint32) []uint32 {
	parent, ok := bt.childToParent[pageNo]
	if !ok {
		return nil
	}
	children := bt.interiorChildren[parent]
	result := make([]uint32, len(children))
	copy(result, children)
	return result
}

// Reset clears all tracked B-tree structure.
func (bt *btreeTracker) Reset() {
	bt.interiorChildren = make(map[uint32][]uint32)
	bt.childToParent = make(map[uint32]uint32)
}

// evictOne removes the first interior page found (no LRU ordering needed,
// just a bound to prevent unbounded memory growth).
func (bt *btreeTracker) evictOne() {
	for pgno, children := range bt.interiorChildren {
		for _, child := range children {
			delete(bt.childToParent, child)
		}
		delete(bt.interiorChildren, pgno)
		return
	}
}
