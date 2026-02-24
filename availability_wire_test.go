package prefetch

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestEncodeDecodeSnapshot_Empty(t *testing.T) {
	data := EncodeSnapshot(nil)
	bitmapData, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(bitmapData) != 0 {
		t.Fatalf("expected empty bitmap data, got %d bytes", len(bitmapData))
	}
}

func TestEncodeDecodeSnapshot_RoundTrip(t *testing.T) {
	bm := roaring64.New()
	bm.Add(logicalAddr(42, 0))
	bm.Add(logicalAddr(42, 5))
	bm.Add(logicalAddr(42, 15))
	original, err := bm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	data := EncodeSnapshot(original)
	decoded, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}

	bm2 := roaring64.New()
	if err := bm2.UnmarshalBinary(decoded); err != nil {
		t.Fatal(err)
	}

	if bm2.GetCardinality() != 3 {
		t.Fatalf("expected 3 entries, got %d", bm2.GetCardinality())
	}
	if !bm2.Contains(logicalAddr(42, 0)) {
		t.Fatal("missing addr(42,0)")
	}
	if !bm2.Contains(logicalAddr(42, 15)) {
		t.Fatal("missing addr(42,15)")
	}
}

func TestEncodeDecodeDelta_Add(t *testing.T) {
	addr := logicalAddr(100, 5)
	data := EncodeDelta(DeltaAdd, addr)
	op, logAddr, err := DecodeDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	if op != DeltaAdd {
		t.Fatalf("op = %d, want DeltaAdd", op)
	}
	if logAddr != addr {
		t.Fatalf("logicalAddr = %d, want %d", logAddr, addr)
	}
}

func TestEncodeDecodeDelta_Remove(t *testing.T) {
	addr := logicalAddr(200, 10)
	data := EncodeDelta(DeltaRemove, addr)
	op, logAddr, err := DecodeDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	if op != DeltaRemove {
		t.Fatalf("op = %d, want DeltaRemove", op)
	}
	if logAddr != addr {
		t.Fatalf("logicalAddr = %d, want %d", logAddr, addr)
	}
}

func TestDecodeSnapshot_TooShort(t *testing.T) {
	_, err := DecodeSnapshot([]byte{0x13})
	if err == nil {
		t.Fatal("expected error for too-short snapshot")
	}
}

func TestDecodeSnapshot_InvalidHeader(t *testing.T) {
	_, err := DecodeSnapshot([]byte{0xFF, 0x01})
	if err == nil {
		t.Fatal("expected error for invalid header")
	}
}

func TestDecodeDelta_TooShort(t *testing.T) {
	_, _, err := DecodeDelta([]byte{0x13, 0x02, 0x01})
	if err == nil {
		t.Fatal("expected error for too-short delta")
	}
}

func TestDecodeDelta_InvalidHeader(t *testing.T) {
	_, _, err := DecodeDelta([]byte{0xFF, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for invalid header")
	}
}

func TestDelta_WireSize(t *testing.T) {
	data := EncodeDelta(DeltaAdd, logicalAddr(5, 0))
	// [1B type][1B sub][1B op][8B logicalAddr] = 11 bytes
	if len(data) != 11 {
		t.Fatalf("delta size = %d, want 11", len(data))
	}
}

func TestSnapshot_WireSize(t *testing.T) {
	bm := roaring64.New()
	bm.Add(logicalAddr(5, 0))
	bitmapData, _ := bm.MarshalBinary()
	data := EncodeSnapshot(bitmapData)
	// [1B type][1B sub][bitmap bytes]
	expected := 2 + len(bitmapData)
	if len(data) != expected {
		t.Fatalf("snapshot size = %d, want %d", len(data), expected)
	}
}
