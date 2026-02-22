package prefetch

import (
	"encoding/binary"
	"fmt"
)

// parseInteriorPage parses a SQLite interior B-tree page (flag 0x05 for table,
// 0x02 for index) and returns child page numbers in left-to-right traversal
// order. Both page types have identical child-pointer layout. Page 1 has a
// 100-byte database header before the B-tree header; pass pageNo=1 to handle it.
func parseInteriorPage(data []byte, pageNo uint32) ([]uint32, error) {
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
	if flag != 0x05 && flag != 0x02 {
		return nil, fmt.Errorf("btree: not an interior page (flag=0x%02x)", flag)
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

// readVarint decodes a SQLite variable-length integer from b.
// Returns the decoded value and the number of bytes consumed.
// SQLite varints are 1-9 bytes, big-endian, 7 bits per byte with high bit as continuation.
func readVarint(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, 0
	}
	// Fast path: single byte (value 0-127).
	if b[0] < 0x80 {
		return uint64(b[0]), 1
	}
	var val uint64
	for i := 0; i < 8; i++ {
		if i >= len(b) {
			return val, i
		}
		val = (val << 7) | uint64(b[i]&0x7F)
		if b[i] < 0x80 {
			return val, i + 1
		}
	}
	// 9th byte: all 8 bits are used.
	if len(b) > 8 {
		val = (val << 8) | uint64(b[8])
		return val, 9
	}
	return val, len(b)
}

// parseLeafTableOverflows parses a SQLite leaf table page (flag 0x0D) and
// returns the first overflow page numbers for cells whose payload exceeds
// the local storage limit. sqlitePgno is the 1-based SQLite page number.
func parseLeafTableOverflows(data []byte, sqlitePgno uint32) []uint32 {
	hdrOff := 0
	if sqlitePgno == 1 {
		hdrOff = 100
	}

	if len(data) < hdrOff+8 {
		return nil
	}
	if data[hdrOff] != 0x0D {
		return nil
	}

	cellCount := int(binary.BigEndian.Uint16(data[hdrOff+3 : hdrOff+5]))
	if cellCount == 0 {
		return nil
	}

	usable := len(data) // page size = usable size (no reserved bytes assumed)
	// maxLocal for table leaf: (usable-35)*64/255 - 23
	maxLocal := (usable-35)*64/255 - 23
	// minLocal for table leaf: (usable-12)*32/255 - 23
	minLocal := (usable-12)*32/255 - 23

	// Leaf page header is 8 bytes.
	ptrArrayStart := hdrOff + 8
	ptrArrayEnd := ptrArrayStart + cellCount*2
	if ptrArrayEnd > len(data) {
		return nil
	}

	var overflows []uint32
	for i := 0; i < cellCount; i++ {
		cellOff := int(binary.BigEndian.Uint16(data[ptrArrayStart+i*2 : ptrArrayStart+i*2+2]))
		if cellOff >= len(data) {
			continue
		}

		// Cell format: varint payloadSize, varint rowid, payload...
		payloadSize, n1 := readVarint(data[cellOff:])
		if n1 == 0 {
			continue
		}
		_, n2 := readVarint(data[cellOff+n1:])
		payloadStart := cellOff + n1 + n2

		if int(payloadSize) <= maxLocal {
			continue // fits entirely on page, no overflow
		}

		// Compute local payload size using SQLite's formula.
		// localSize = minLocal + (payloadSize - minLocal) % (usable - 4)
		// if localSize > maxLocal: localSize = minLocal
		localSize := minLocal + (int(payloadSize)-minLocal)%(usable-4)
		if localSize > maxLocal {
			localSize = minLocal
		}

		// First overflow page number is 4 bytes at payloadStart + localSize.
		ovflOff := payloadStart + localSize
		if ovflOff+4 > len(data) {
			continue
		}
		ovflPgno := binary.BigEndian.Uint32(data[ovflOff : ovflOff+4])
		if ovflPgno > 0 {
			overflows = append(overflows, ovflPgno)
		}
	}
	return overflows
}

// btreeTracker learns B-tree structure from fetched pages and predicts
// upcoming leaf page accesses. Not thread-safe — caller must synchronize
// (ReadaheadEngine holds its mutex during OnPageAccess/OnFetchComplete calls).
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

// OnFetchComplete inspects fetched page data. If it's an interior page (table
// or index), parses the child pointers and stores the mapping. pageNo is 0-based
// (Prefetcher convention); child pointers in the B-tree are 1-based SQLite
// pgno values. This method converts children to 0-based before storing.
func (bt *btreeTracker) OnFetchComplete(pageNo uint32, data []byte) {
	// Convert 0-based pageNo to 1-based SQLite pgno for the parser.
	children, err := parseInteriorPage(data, pageNo+1)
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
