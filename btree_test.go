package prefetch

import (
	"encoding/binary"
	"testing"
)

// buildInteriorTablePage constructs a synthetic SQLite interior table B-tree
// page with the given child page numbers. The last entry in childPgNos becomes
// the rightmost pointer (stored in the header), the rest become cell entries.
func buildInteriorTablePage(pageSize int, childPgNos []uint32) []byte {
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

func TestParseInteriorTablePage_Basic(t *testing.T) {
	children := []uint32{10, 20, 30, 40, 50}
	page := buildInteriorTablePage(4096, children)

	got, err := parseInteriorTablePage(page, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(children) {
		t.Fatalf("got %d children, want %d", len(got), len(children))
	}
	for i, c := range children {
		if got[i] != c {
			t.Errorf("child[%d] = %d, want %d", i, got[i], c)
		}
	}
}

func TestParseInteriorTablePage_Page1Offset(t *testing.T) {
	// Page 1 has a 100-byte DB header, then the B-tree header at offset 100.
	// Cell offsets in the cell pointer array are absolute (from byte 0).
	page1 := make([]byte, 4096)

	hdrOff := 100
	page1[hdrOff] = 0x05                                                    // interior table b-tree
	binary.BigEndian.PutUint16(page1[hdrOff+3:hdrOff+5], 1)                 // 1 cell
	binary.BigEndian.PutUint32(page1[hdrOff+8:hdrOff+12], 10)               // rightmost child = 10
	cellOff := 4096 - 5                                                      // cell body near end of page
	binary.BigEndian.PutUint16(page1[hdrOff+12:hdrOff+14], uint16(cellOff)) // cell pointer (absolute)
	binary.BigEndian.PutUint32(page1[cellOff:cellOff+4], 5)                 // child = 5
	page1[cellOff+4] = 1                                                    // varint rowid

	got, err := parseInteriorTablePage(page1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != 5 || got[1] != 10 {
		t.Fatalf("got %v, want [5 10]", got)
	}
}

func TestParseInteriorTablePage_SingleCell(t *testing.T) {
	// Minimum: 1 cell + rightmost pointer = 2 children.
	children := []uint32{7, 99}
	page := buildInteriorTablePage(4096, children)

	got, err := parseInteriorTablePage(page, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != 7 || got[1] != 99 {
		t.Fatalf("got %v, want [7 99]", got)
	}
}

func TestParseInteriorTablePage_NotInterior(t *testing.T) {
	page := make([]byte, 4096)
	page[0] = 0x0D // leaf table page, not interior

	_, err := parseInteriorTablePage(page, 2)
	if err == nil {
		t.Fatal("expected error for non-interior page")
	}
}

func TestParseInteriorTablePage_TooSmall(t *testing.T) {
	_, err := parseInteriorTablePage(make([]byte, 10), 2)
	if err == nil {
		t.Fatal("expected error for undersized page")
	}
}

func TestParseInteriorTablePage_ZeroCells(t *testing.T) {
	page := make([]byte, 4096)
	page[0] = 0x05
	// cellCount = 0 at bytes 3-4 (already zero)

	_, err := parseInteriorTablePage(page, 2)
	if err == nil {
		t.Fatal("expected error for zero cells")
	}
}

func TestParseInteriorTablePage_CellOffsetOutOfBounds(t *testing.T) {
	page := make([]byte, 4096)
	page[0] = 0x05
	binary.BigEndian.PutUint16(page[3:5], 1) // 1 cell
	binary.BigEndian.PutUint32(page[8:12], 99)
	// Cell offset pointing beyond page.
	binary.BigEndian.PutUint16(page[12:14], 4095)

	_, err := parseInteriorTablePage(page, 2)
	if err == nil {
		t.Fatal("expected error for out-of-bounds cell offset")
	}
}

func TestBtreeTracker_Siblings(t *testing.T) {
	bt := newBtreeTracker(1024)
	// 1-based [11, 21, 31, 41, 51] → 0-based [10, 20, 30, 40, 50]
	children := []uint32{11, 21, 31, 41, 51}
	page := buildInteriorTablePage(4096, children)
	bt.OnFetchComplete(2, page)

	// Siblings of any child should return all children.
	sibs := bt.Siblings(20)
	if len(sibs) != 5 {
		t.Fatalf("expected 5 siblings, got %d", len(sibs))
	}
	want := []uint32{10, 20, 30, 40, 50}
	for i, s := range sibs {
		if s != want[i] {
			t.Errorf("sibling[%d] = %d, want %d", i, s, want[i])
		}
	}

	// First child also returns all siblings.
	sibs = bt.Siblings(10)
	if len(sibs) != 5 {
		t.Fatalf("expected 5 siblings for first child, got %d", len(sibs))
	}
}

func TestBtreeTracker_Siblings_Unknown(t *testing.T) {
	bt := newBtreeTracker(1024)
	sibs := bt.Siblings(999)
	if sibs != nil {
		t.Fatalf("expected nil for unknown page, got %v", sibs)
	}
}

func TestBtreeTracker_PredictSiblings(t *testing.T) {
	bt := newBtreeTracker(1024)
	// Page data contains 1-based SQLite pgno values; OnFetchComplete converts
	// them to 0-based: [11,21,31,41,51] → [10,20,30,40,50].
	children := []uint32{11, 21, 31, 41, 51}
	page := buildInteriorTablePage(4096, children)

	bt.OnFetchComplete(2, page) // 0-based page 2 is the interior page

	// Predict from first child (0-based 10) → remaining siblings.
	pages, result := bt.Predict(10)
	if result != PredictOK {
		t.Fatal("expected prediction for child 10")
	}
	want := []uint32{20, 30, 40, 50}
	if len(pages) != len(want) {
		t.Fatalf("got %d pages, want %d", len(pages), len(want))
	}
	for i, p := range want {
		if pages[i] != p {
			t.Errorf("pages[%d] = %d, want %d", i, pages[i], p)
		}
	}

	// Predict from middle child.
	pages, result = bt.Predict(30)
	if result != PredictOK {
		t.Fatal("expected prediction for child 30")
	}
	want = []uint32{40, 50}
	if len(pages) != len(want) {
		t.Fatalf("got %v, want %v", pages, want)
	}

	// Predict from last child → no remaining siblings.
	_, result = bt.Predict(50)
	if result != PredictLastChild {
		t.Fatalf("expected PredictLastChild for last child, got %d", result)
	}

	// Unknown page → no prediction.
	_, result = bt.Predict(999)
	if result != PredictNotChild {
		t.Fatalf("expected PredictNotChild for unknown page, got %d", result)
	}
}

func TestBtreeTracker_NonInteriorPageIgnored(t *testing.T) {
	bt := newBtreeTracker(1024)
	leaf := make([]byte, 4096)
	leaf[0] = 0x0D // leaf table page

	bt.OnFetchComplete(5, leaf)

	// Should have no data.
	_, result := bt.Predict(5)
	if result == PredictOK {
		t.Fatal("leaf page should not produce predictions")
	}
}

func TestBtreeTracker_Reset(t *testing.T) {
	bt := newBtreeTracker(1024)
	// 1-based [11,21,31] → 0-based [10,20,30]
	children := []uint32{11, 21, 31}
	page := buildInteriorTablePage(4096, children)
	bt.OnFetchComplete(2, page)

	// Verify prediction works before reset.
	_, result := bt.Predict(10)
	if result != PredictOK {
		t.Fatal("expected prediction before reset")
	}

	bt.Reset()

	_, result = bt.Predict(10)
	if result == PredictOK {
		t.Fatal("expected no prediction after reset")
	}
}

func TestBtreeTracker_MemoryBound(t *testing.T) {
	bt := newBtreeTracker(2) // max 2 interior pages

	// Add 3 interior pages — should evict one.
	for pgno := uint32(1); pgno <= 3; pgno++ {
		children := []uint32{pgno * 100, pgno*100 + 1}
		page := buildInteriorTablePage(4096, children)
		bt.OnFetchComplete(pgno, page)
	}

	if len(bt.interiorChildren) > 2 {
		t.Fatalf("expected at most 2 tracked interior pages, got %d", len(bt.interiorChildren))
	}
}

func TestBtreeTracker_MultiLevelLookahead(t *testing.T) {
	bt := newBtreeTracker(1024)

	// Root page (0-based 3) has interior children.
	// 1-based [101, 201, 301] → 0-based [100, 200, 300].
	rootChildren := []uint32{101, 201, 301}
	rootPage := buildInteriorTablePage(4096, rootChildren)
	bt.OnFetchComplete(3, rootPage)

	// Interior page (0-based 100) has leaf children.
	// 1-based [11, 12, 13] → 0-based [10, 11, 12].
	int100Children := []uint32{11, 12, 13}
	int100Page := buildInteriorTablePage(4096, int100Children)
	bt.OnFetchComplete(100, int100Page)

	// Predict from leaf 11 (only 1 remaining sibling = 12, <= lookaheadThreshold).
	// Should also include next interior sibling (200) for lookahead.
	pages, result := bt.Predict(11)
	if result != PredictOK {
		t.Fatal("expected prediction for leaf 11")
	}

	// Should contain: remaining leaf sibling (12) + next interior sibling (200).
	hasLeaf12 := false
	hasInterior200 := false
	for _, p := range pages {
		if p == 12 {
			hasLeaf12 = true
		}
		if p == 200 {
			hasInterior200 = true
		}
	}
	if !hasLeaf12 {
		t.Errorf("expected leaf 12 in prediction, got %v", pages)
	}
	if !hasInterior200 {
		t.Errorf("expected interior 200 (lookahead) in prediction, got %v", pages)
	}
}

func TestBtreeTracker_NoLookaheadWhenManyRemaining(t *testing.T) {
	bt := newBtreeTracker(1024)

	// 1-based [101, 201, 301] → 0-based [100, 200, 300]
	rootChildren := []uint32{101, 201, 301}
	rootPage := buildInteriorTablePage(4096, rootChildren)
	bt.OnFetchComplete(3, rootPage)

	// Interior page 100 has many leaf children (1-based 11..30 → 0-based 10..29).
	leaves := make([]uint32, 20)
	for i := range leaves {
		leaves[i] = uint32(11 + i)
	}
	int100Page := buildInteriorTablePage(4096, leaves)
	bt.OnFetchComplete(100, int100Page)

	// Predict from leaf 10 — 19 remaining siblings, well above threshold.
	// Should NOT include interior page 200.
	pages, result := bt.Predict(10)
	if result != PredictOK {
		t.Fatal("expected prediction for leaf 10")
	}
	for _, p := range pages {
		if p == 200 {
			t.Fatal("should not include lookahead interior page when many siblings remain")
		}
	}
}

func TestBtreeTracker_LookaheadLastInterior(t *testing.T) {
	bt := newBtreeTracker(1024)

	// Root has 1-based [101, 201] → 0-based [100, 200]. Interior 200 is last.
	rootChildren := []uint32{101, 201}
	rootPage := buildInteriorTablePage(4096, rootChildren)
	bt.OnFetchComplete(3, rootPage)

	// 1-based [51, 52] → 0-based [50, 51]
	int200Children := []uint32{51, 52}
	int200Page := buildInteriorTablePage(4096, int200Children)
	bt.OnFetchComplete(200, int200Page)

	// Predict from leaf 50 — only 1 sibling, but parent (200) is last
	// in grandparent's children, so no lookahead interior to add.
	pages, result := bt.Predict(50)
	if result != PredictOK {
		t.Fatal("expected prediction for leaf 50")
	}
	// Should just have leaf 51, no lookahead.
	if len(pages) != 1 || pages[0] != 51 {
		t.Fatalf("got %v, want [51]", pages)
	}
}
