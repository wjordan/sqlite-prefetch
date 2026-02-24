package prefetch

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestEncodeDecodeSnapshot_Empty(t *testing.T) {
	encoded := EncodeSnapshot(nil)
	// Strip the type prefix (transport dispatch strips it before delivery).
	postDispatch := encoded[1:]
	bitmapData, err := DecodeSnapshot(postDispatch)
	if err != nil {
		t.Fatal(err)
	}
	if len(bitmapData) != 0 {
		t.Fatalf("expected empty bitmap data, got %d bytes", len(bitmapData))
	}
}

func TestEncodeDecodeSnapshot_RoundTrip(t *testing.T) {
	bm := roaring64.New()
	bm.Add(42)
	bm.Add(100)
	bm.Add(999)
	original, err := bm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	encoded := EncodeSnapshot(original)
	// Strip the type prefix for decode.
	postDispatch := encoded[1:]
	decoded, err := DecodeSnapshot(postDispatch)
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
	if !bm2.Contains(42) {
		t.Fatal("missing page 42")
	}
	if !bm2.Contains(999) {
		t.Fatal("missing page 999")
	}
}

func TestEncodeDecodeDelta_Add(t *testing.T) {
	encoded := EncodeDelta(DeltaAdd, 42)
	// Strip the type prefix for decode.
	postDispatch := encoded[1:]
	op, pageNo, err := DecodeDelta(postDispatch)
	if err != nil {
		t.Fatal(err)
	}
	if op != DeltaAdd {
		t.Fatalf("op = %d, want DeltaAdd", op)
	}
	if pageNo != 42 {
		t.Fatalf("pageNo = %d, want 42", pageNo)
	}
}

func TestEncodeDecodeDelta_Remove(t *testing.T) {
	encoded := EncodeDelta(DeltaRemove, 200)
	// Strip the type prefix for decode.
	postDispatch := encoded[1:]
	op, pageNo, err := DecodeDelta(postDispatch)
	if err != nil {
		t.Fatal(err)
	}
	if op != DeltaRemove {
		t.Fatalf("op = %d, want DeltaRemove", op)
	}
	if pageNo != 200 {
		t.Fatalf("pageNo = %d, want 200", pageNo)
	}
}

func TestDecodeSnapshot_TooShort(t *testing.T) {
	_, err := DecodeSnapshot(nil)
	if err == nil {
		t.Fatal("expected error for too-short snapshot")
	}
}

func TestDecodeSnapshot_InvalidSubType(t *testing.T) {
	_, err := DecodeSnapshot([]byte{0xFF})
	if err == nil {
		t.Fatal("expected error for invalid sub-type")
	}
}

func TestDecodeDelta_TooShort(t *testing.T) {
	_, _, err := DecodeDelta([]byte{0x02, 0x01})
	if err == nil {
		t.Fatal("expected error for too-short delta")
	}
}

func TestDecodeDelta_InvalidSubType(t *testing.T) {
	_, _, err := DecodeDelta([]byte{0xFF, 0x01, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for invalid sub-type")
	}
}

func TestDelta_WireSize(t *testing.T) {
	data := EncodeDelta(DeltaAdd, 5)
	// [1B type][1B sub][1B op][4B pageNo] = 7 bytes
	if len(data) != 7 {
		t.Fatalf("delta size = %d, want 7", len(data))
	}
}

func TestSnapshot_WireSize(t *testing.T) {
	bm := roaring64.New()
	bm.Add(5)
	bitmapData, _ := bm.MarshalBinary()
	data := EncodeSnapshot(bitmapData)
	// [1B type][1B sub][bitmap bytes]
	expected := 2 + len(bitmapData)
	if len(data) != expected {
		t.Fatalf("snapshot size = %d, want %d", len(data), expected)
	}
}
