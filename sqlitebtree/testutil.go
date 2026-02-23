package sqlitebtree

import "encoding/binary"

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
