// Package sqlitebtree parses SQLite B-tree page structure and tracks
// parent-child relationships for predicting upcoming page accesses.
//
// SQLite interior pages (table flag 0x05, index flag 0x02) contain arrays
// of child page pointers. This package parses those pages and exposes the
// logical structure: which pages are siblings, what their parent is, and
// what position each child occupies in the B-tree traversal order.
//
// The package is pure computation — no I/O, no concurrency, no network.
// Callers feed it page data as it becomes available and query the resulting
// structure.
package sqlitebtree

import (
	"encoding/binary"
	"fmt"
)

// ParseInteriorPage parses a SQLite interior B-tree page (flag 0x05 for table,
// 0x02 for index) and returns child page numbers in left-to-right traversal
// order. Both page types have identical child-pointer layout. Page 1 has a
// 100-byte database header before the B-tree header; pass pageNo=1 to handle it.
func ParseInteriorPage(data []byte, pageNo uint32) ([]uint32, error) {
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

// ReadVarint decodes a SQLite variable-length integer from b.
// Returns the decoded value and the number of bytes consumed.
// SQLite varints are 1-9 bytes, big-endian, 7 bits per byte with high bit as continuation.
func ReadVarint(b []byte) (uint64, int) {
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

// ParseLeafTableOverflows parses a SQLite leaf table page (flag 0x0D) and
// returns the first overflow page numbers for cells whose payload exceeds
// the local storage limit. sqlitePgno is the 1-based SQLite page number.
func ParseLeafTableOverflows(data []byte, sqlitePgno uint32) []uint32 {
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
		payloadSize, n1 := ReadVarint(data[cellOff:])
		if n1 == 0 {
			continue
		}
		_, n2 := ReadVarint(data[cellOff+n1:])
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

// PredictResult indicates why Predict returned its result.
type PredictResult int

const (
	PredictOK        PredictResult = iota // predicted siblings
	PredictNotChild                       // page not in childToParent
	PredictLastChild                      // page is the last child (no remaining siblings)
)

const lookaheadThreshold = 10

// Tracker learns B-tree structure from fetched pages and predicts
// upcoming leaf page accesses. Not thread-safe — caller must synchronize.
type Tracker struct {
	interiorChildren map[uint32][]uint32 // parent pgno → ordered child list
	childToParent    map[uint32]uint32   // child pgno → parent pgno
	maxTracked       int                 // max interior pages to track
}

// NewTracker creates a Tracker that tracks up to maxTracked interior pages.
func NewTracker(maxTracked int) *Tracker {
	return &Tracker{
		interiorChildren: make(map[uint32][]uint32),
		childToParent:    make(map[uint32]uint32),
		maxTracked:       maxTracked,
	}
}

// OnFetchComplete inspects fetched page data. If it's an interior page (table
// or index), parses the child pointers and stores the mapping. pageNo is 0-based
// (caller convention); child pointers in the B-tree are 1-based SQLite
// pgno values. This method converts children to 0-based before storing.
func (t *Tracker) OnFetchComplete(pageNo uint32, data []byte) {
	// Convert 0-based pageNo to 1-based SQLite pgno for the parser.
	children, err := ParseInteriorPage(data, pageNo+1)
	if err != nil {
		return // not an interior page or malformed — silently ignore
	}

	// Evict if over limit.
	for len(t.interiorChildren) >= t.maxTracked {
		t.evictOne()
	}

	// Convert 1-based SQLite pgno children to 0-based.
	converted := make([]uint32, len(children))
	for i, child := range children {
		converted[i] = child - 1
	}

	t.interiorChildren[pageNo] = converted
	for _, child := range converted {
		t.childToParent[child] = pageNo
	}
}

// Predict returns the remaining sibling pages after pageNo in its parent's
// child list. When remaining siblings <= lookaheadThreshold, also appends the
// next interior sibling page (multi-level lookahead) so it gets prefetched
// and parsed before the reader descends into it.
func (t *Tracker) Predict(pageNo uint32) ([]uint32, PredictResult) {
	parent, ok := t.childToParent[pageNo]
	if !ok {
		return nil, PredictNotChild
	}
	children := t.interiorChildren[parent]

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
	// parsed and ready before the reader descends into it.
	if len(remaining) <= lookaheadThreshold {
		if nextInterior, ok := t.nextInteriorSibling(parent); ok {
			result = append(result, nextInterior)
		}
	}

	return result, PredictOK
}

// ChildPosition returns the interior parent page and child index for a page.
// Returns ok=false if the page's parent hasn't been parsed yet.
func (t *Tracker) ChildPosition(pageNo uint32) (parent uint32, index int, ok bool) {
	parent, ok = t.childToParent[pageNo]
	if !ok {
		return 0, -1, false
	}
	children := t.interiorChildren[parent]
	for i, c := range children {
		if c == pageNo {
			return parent, i, true
		}
	}
	return 0, -1, false
}

// Siblings returns all children of pageNo's parent (the full sibling set),
// including pageNo itself. Returns nil if pageNo is not a known child.
func (t *Tracker) Siblings(pageNo uint32) []uint32 {
	parent, ok := t.childToParent[pageNo]
	if !ok {
		return nil
	}
	children := t.interiorChildren[parent]
	result := make([]uint32, len(children))
	copy(result, children)
	return result
}

// Reset clears all tracked B-tree structure.
func (t *Tracker) Reset() {
	t.interiorChildren = make(map[uint32][]uint32)
	t.childToParent = make(map[uint32]uint32)
}

// nextInteriorSibling finds the next sibling of interiorPgNo in its parent's
// child list.
func (t *Tracker) nextInteriorSibling(interiorPgNo uint32) (uint32, bool) {
	grandparent, ok := t.childToParent[interiorPgNo]
	if !ok {
		return 0, false
	}
	gpChildren := t.interiorChildren[grandparent]
	for i, c := range gpChildren {
		if c == interiorPgNo && i < len(gpChildren)-1 {
			return gpChildren[i+1], true
		}
	}
	return 0, false
}

// evictOne removes the first interior page found (no LRU ordering needed,
// just a bound to prevent unbounded memory growth).
func (t *Tracker) evictOne() {
	for pgno, children := range t.interiorChildren {
		for _, child := range children {
			delete(t.childToParent, child)
		}
		delete(t.interiorChildren, pgno)
		return
	}
}

// BuildInteriorTablePage constructs a synthetic SQLite interior table B-tree
// page with the given child page numbers. The last entry in childPgNos becomes
// the rightmost pointer (stored in the header), the rest become cell entries.
func BuildInteriorTablePage(pageSize int, childPgNos []uint32) []byte {
	if len(childPgNos) < 2 {
		panic("need at least 2 children (1 cell + rightmost)")
	}
	page := make([]byte, pageSize)

	// B-tree page header (12 bytes for interior pages).
	page[0] = 0x05 // interior table b-tree
	cellCount := len(childPgNos) - 1
	binary.BigEndian.PutUint16(page[3:5], uint16(cellCount))
	// Rightmost child pointer at offset 8.
	binary.BigEndian.PutUint32(page[8:12], childPgNos[len(childPgNos)-1])

	// Cell pointer array starts at offset 12 (2 bytes each).
	// Cell bodies are written from the end of the page backward.
	cellBodyStart := pageSize
	for i := 0; i < cellCount; i++ {
		// Each cell: 4-byte child pgno + 1-byte varint rowid.
		cellSize := 5 // 4 bytes pgno + 1 byte minimal varint
		cellBodyStart -= cellSize
		// Cell pointer.
		binary.BigEndian.PutUint16(page[12+i*2:14+i*2], uint16(cellBodyStart))
		// Cell body: child page number.
		binary.BigEndian.PutUint32(page[cellBodyStart:cellBodyStart+4], childPgNos[i])
		// Varint rowid (minimal: single byte, value = i+1).
		page[cellBodyStart+4] = byte(i + 1)
	}

	return page
}

// LeafCell describes a cell for BuildLeafTablePage.
type LeafCell struct {
	PayloadSize  int
	Rowid        int
	OverflowPgno uint32 // 1-based; 0 means no overflow
}

// BuildLeafTablePage constructs a leaf table page (flag 0x0D) with the given cells.
func BuildLeafTablePage(pageSize int, cells []LeafCell) []byte {
	page := make([]byte, pageSize)
	page[0] = 0x0D // leaf table b-tree
	cellCount := len(cells)
	binary.BigEndian.PutUint16(page[3:5], uint16(cellCount))

	// Leaf page header is 8 bytes. Cell pointer array follows.
	ptrArrayStart := 8
	cellBodyStart := pageSize

	for i, cell := range cells {
		// Encode payload size varint.
		payloadVarint := EncodeVarint(uint64(cell.PayloadSize))
		// Encode rowid varint.
		rowidVarint := EncodeVarint(uint64(cell.Rowid))

		usable := pageSize
		maxLocal := (usable-35)*64/255 - 23
		minLocal := (usable-12)*32/255 - 23

		var localSize int
		var ovflPgno uint32
		if cell.PayloadSize > maxLocal {
			localSize = minLocal + (cell.PayloadSize-minLocal)%(usable-4)
			if localSize > maxLocal {
				localSize = minLocal
			}
			ovflPgno = cell.OverflowPgno
		} else {
			localSize = cell.PayloadSize
		}

		cellSize := len(payloadVarint) + len(rowidVarint) + localSize
		if ovflPgno > 0 {
			cellSize += 4
		}
		cellBodyStart -= cellSize

		// Cell pointer.
		binary.BigEndian.PutUint16(page[ptrArrayStart+i*2:ptrArrayStart+i*2+2], uint16(cellBodyStart))

		// Cell body.
		off := cellBodyStart
		copy(page[off:], payloadVarint)
		off += len(payloadVarint)
		copy(page[off:], rowidVarint)
		off += len(rowidVarint)
		// Fill local payload with dummy data.
		for j := 0; j < localSize; j++ {
			page[off+j] = 0xAA
		}
		off += localSize
		if ovflPgno > 0 {
			binary.BigEndian.PutUint32(page[off:off+4], ovflPgno)
		}
	}

	return page
}

// EncodeVarint encodes a uint64 as a SQLite variable-length integer.
func EncodeVarint(v uint64) []byte {
	if v <= 127 {
		return []byte{byte(v)}
	}
	var buf [9]byte
	n := 8
	buf[n] = byte(v & 0x7F)
	v >>= 7
	for v > 0 {
		n--
		buf[n] = byte(v&0x7F) | 0x80
		v >>= 7
	}
	return buf[n:]
}
