package prefetch

import (
	"testing"
)

func TestEncodeDecodeSnapshot_Empty(t *testing.T) {
	data := EncodeSnapshot(nil)
	pages, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 0 {
		t.Fatalf("expected 0 pages, got %d", len(pages))
	}
}

func TestEncodeDecodeSnapshot_SinglePage(t *testing.T) {
	original := []PageAvailability{
		{
			InteriorPage: 42,
			Extents:      []ChildExtent{{Start: 0, Count: 10}, {Start: 15, Count: 5}},
		},
	}
	data := EncodeSnapshot(original)
	decoded, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded) != 1 {
		t.Fatalf("expected 1 page, got %d", len(decoded))
	}
	if decoded[0].InteriorPage != 42 {
		t.Fatalf("interior page = %d, want 42", decoded[0].InteriorPage)
	}
	if len(decoded[0].Extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(decoded[0].Extents))
	}
	if decoded[0].Extents[0].Start != 0 || decoded[0].Extents[0].Count != 10 {
		t.Fatalf("extent[0] = %+v, want {0, 10}", decoded[0].Extents[0])
	}
	if decoded[0].Extents[1].Start != 15 || decoded[0].Extents[1].Count != 5 {
		t.Fatalf("extent[1] = %+v, want {15, 5}", decoded[0].Extents[1])
	}
}

func TestEncodeDecodeSnapshot_MultiplePages(t *testing.T) {
	original := []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 100}}},
		{InteriorPage: 6, Extents: []ChildExtent{{Start: 10, Count: 50}}},
		{InteriorPage: 7, Extents: []ChildExtent{{Start: 0, Count: 1}, {Start: 5, Count: 2}, {Start: 99, Count: 1}}},
	}
	data := EncodeSnapshot(original)
	decoded, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded) != 3 {
		t.Fatalf("expected 3 pages, got %d", len(decoded))
	}
	for i, orig := range original {
		if decoded[i].InteriorPage != orig.InteriorPage {
			t.Errorf("page[%d].InteriorPage = %d, want %d", i, decoded[i].InteriorPage, orig.InteriorPage)
		}
		if len(decoded[i].Extents) != len(orig.Extents) {
			t.Errorf("page[%d] extents count = %d, want %d", i, len(decoded[i].Extents), len(orig.Extents))
			continue
		}
		for j, ext := range orig.Extents {
			if decoded[i].Extents[j] != ext {
				t.Errorf("page[%d].extent[%d] = %+v, want %+v", i, j, decoded[i].Extents[j], ext)
			}
		}
	}
}

func TestEncodeDecodeDelta_Add(t *testing.T) {
	original := AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 100,
		Extents:      []ChildExtent{{Start: 5, Count: 20}},
	}
	data := EncodeDelta(original)
	decoded, err := DecodeDelta(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Op != DeltaAdd {
		t.Fatalf("op = %d, want DeltaAdd", decoded.Op)
	}
	if decoded.InteriorPage != 100 {
		t.Fatalf("interior page = %d, want 100", decoded.InteriorPage)
	}
	if len(decoded.Extents) != 1 {
		t.Fatalf("expected 1 extent, got %d", len(decoded.Extents))
	}
	if decoded.Extents[0].Start != 5 || decoded.Extents[0].Count != 20 {
		t.Fatalf("extent = %+v, want {5, 20}", decoded.Extents[0])
	}
}

func TestEncodeDecodeDelta_Remove(t *testing.T) {
	original := AvailabilityDelta{
		Op:           DeltaRemove,
		InteriorPage: 200,
		Extents:      []ChildExtent{{Start: 0, Count: 5}, {Start: 10, Count: 3}},
	}
	data := EncodeDelta(original)
	decoded, err := DecodeDelta(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Op != DeltaRemove {
		t.Fatalf("op = %d, want DeltaRemove", decoded.Op)
	}
	if decoded.InteriorPage != 200 {
		t.Fatalf("interior page = %d, want 200", decoded.InteriorPage)
	}
	if len(decoded.Extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(decoded.Extents))
	}
}

func TestDecodeSnapshot_TooShort(t *testing.T) {
	_, err := DecodeSnapshot([]byte{0x13, 0x01})
	if err == nil {
		t.Fatal("expected error for too-short snapshot")
	}
}

func TestDecodeSnapshot_InvalidHeader(t *testing.T) {
	_, err := DecodeSnapshot([]byte{0xFF, 0x01, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for invalid header")
	}
}

func TestDecodeDelta_TooShort(t *testing.T) {
	_, err := DecodeDelta([]byte{0x13, 0x02})
	if err == nil {
		t.Fatal("expected error for too-short delta")
	}
}

func TestDecodeDelta_InvalidHeader(t *testing.T) {
	_, err := DecodeDelta([]byte{0xFF, 0x02, 0, 0, 0, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for invalid header")
	}
}

func TestSnapshot_WireSize(t *testing.T) {
	// Verify wire format size matches expected layout.
	// Header: 1 + 1 + 4 = 6
	// Per page: 4 + 2 + 4*numExtents
	pages := []PageAvailability{
		{InteriorPage: 5, Extents: []ChildExtent{{Start: 0, Count: 450}}},
	}
	data := EncodeSnapshot(pages)
	expected := 6 + 4 + 2 + 4 // header + interiorPage + numExtents + 1 extent
	if len(data) != expected {
		t.Fatalf("snapshot size = %d, want %d", len(data), expected)
	}
}

func TestDelta_WireSize(t *testing.T) {
	// Header: 1 + 1 + 4 + 1 + 2 = 9
	// Per extent: 4
	delta := AvailabilityDelta{
		Op:           DeltaAdd,
		InteriorPage: 5,
		Extents:      []ChildExtent{{Start: 0, Count: 100}},
	}
	data := EncodeDelta(delta)
	expected := 9 + 4 // header + 1 extent
	if len(data) != expected {
		t.Fatalf("delta size = %d, want %d", len(data), expected)
	}
}

func TestDecodeSnapshot_Truncated(t *testing.T) {
	// Valid header claiming 1 page, but no page data.
	data := []byte{0x13, 0x01, 0, 0, 0, 1}
	_, err := DecodeSnapshot(data)
	if err == nil {
		t.Fatal("expected error for truncated page data")
	}
}

func TestDecodeDelta_TruncatedExtent(t *testing.T) {
	// Valid header claiming 1 extent, but no extent data.
	data := []byte{0x13, 0x02, 0, 0, 0, 5, 0x01, 0, 1}
	_, err := DecodeDelta(data)
	if err == nil {
		t.Fatal("expected error for truncated extent")
	}
}
