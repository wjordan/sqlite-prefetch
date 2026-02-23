package prefetch

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire protocol constants for availability gossip.
const (
	StreamTypeAvailability byte = 0x13

	AvailSubSnapshot byte = 0x01
	AvailSubDelta    byte = 0x02
)

// EncodeSnapshot encodes a full availability snapshot to the wire format:
//
//	[1B type=0x13][1B sub=0x01][4B numPages]
//	per page: [4B interiorPage][2B numExtents][4B * extents]
//
// Each extent is encoded as [2B start][2B count].
func EncodeSnapshot(pages []PageAvailability) []byte {
	// Calculate size.
	size := 1 + 1 + 4 // type + sub + numPages
	for _, pa := range pages {
		size += 4 + 2 + len(pa.Extents)*4 // interiorPage + numExtents + extents
	}

	buf := make([]byte, size)
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubSnapshot
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(pages)))

	off := 6
	for _, pa := range pages {
		binary.BigEndian.PutUint32(buf[off:off+4], pa.InteriorPage)
		off += 4
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(pa.Extents)))
		off += 2
		for _, ext := range pa.Extents {
			binary.BigEndian.PutUint16(buf[off:off+2], ext.Start)
			binary.BigEndian.PutUint16(buf[off+2:off+4], ext.Count)
			off += 4
		}
	}
	return buf
}

// DecodeSnapshot decodes a full availability snapshot from the wire format.
// The input should include the type and sub bytes.
func DecodeSnapshot(data []byte) ([]PageAvailability, error) {
	if len(data) < 6 {
		return nil, errors.New("snapshot: too short")
	}
	if data[0] != StreamTypeAvailability || data[1] != AvailSubSnapshot {
		return nil, fmt.Errorf("snapshot: invalid header %02x %02x", data[0], data[1])
	}

	numPages := binary.BigEndian.Uint32(data[2:6])
	off := 6
	pages := make([]PageAvailability, 0, numPages)

	for i := uint32(0); i < numPages; i++ {
		if off+6 > len(data) {
			return nil, errors.New("snapshot: truncated page header")
		}
		interiorPage := binary.BigEndian.Uint32(data[off : off+4])
		off += 4
		numExtents := binary.BigEndian.Uint16(data[off : off+2])
		off += 2

		extents := make([]ChildExtent, numExtents)
		for j := range extents {
			if off+4 > len(data) {
				return nil, errors.New("snapshot: truncated extent")
			}
			extents[j].Start = binary.BigEndian.Uint16(data[off : off+2])
			extents[j].Count = binary.BigEndian.Uint16(data[off+2 : off+4])
			off += 4
		}
		pages = append(pages, PageAvailability{
			InteriorPage: interiorPage,
			Extents:      extents,
		})
	}
	return pages, nil
}

// EncodeDelta encodes a single availability delta to the wire format:
//
//	[1B type=0x13][1B sub=0x02][4B interiorPage][1B op][2B numExtents][4B * extents]
func EncodeDelta(delta AvailabilityDelta) []byte {
	size := 1 + 1 + 4 + 1 + 2 + len(delta.Extents)*4
	buf := make([]byte, size)
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubDelta
	binary.BigEndian.PutUint32(buf[2:6], delta.InteriorPage)
	buf[6] = byte(delta.Op)
	binary.BigEndian.PutUint16(buf[7:9], uint16(len(delta.Extents)))

	off := 9
	for _, ext := range delta.Extents {
		binary.BigEndian.PutUint16(buf[off:off+2], ext.Start)
		binary.BigEndian.PutUint16(buf[off+2:off+4], ext.Count)
		off += 4
	}
	return buf
}

// DecodeDelta decodes a single availability delta from the wire format.
// The input should include the type and sub bytes.
func DecodeDelta(data []byte) (AvailabilityDelta, error) {
	if len(data) < 9 {
		return AvailabilityDelta{}, errors.New("delta: too short")
	}
	if data[0] != StreamTypeAvailability || data[1] != AvailSubDelta {
		return AvailabilityDelta{}, fmt.Errorf("delta: invalid header %02x %02x", data[0], data[1])
	}

	delta := AvailabilityDelta{
		InteriorPage: binary.BigEndian.Uint32(data[2:6]),
		Op:           DeltaOp(data[6]),
	}

	numExtents := binary.BigEndian.Uint16(data[7:9])
	off := 9
	delta.Extents = make([]ChildExtent, numExtents)
	for i := range delta.Extents {
		if off+4 > len(data) {
			return AvailabilityDelta{}, errors.New("delta: truncated extent")
		}
		delta.Extents[i].Start = binary.BigEndian.Uint16(data[off : off+2])
		delta.Extents[i].Count = binary.BigEndian.Uint16(data[off+2 : off+4])
		off += 4
	}
	return delta, nil
}
