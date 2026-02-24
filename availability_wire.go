package prefetch

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire protocol constants for availability gossip.
const (
	StreamTypeAvailability byte = 0x16

	AvailSubSnapshot byte = 0x01
	AvailSubDelta    byte = 0x02
)

// EncodeSnapshot encodes a full availability snapshot to the wire format:
//
//	[1B type=0x16][1B sub=0x01][roaring64 MarshalBinary bytes]
//
// The type prefix is used by the transport layer for dispatch.
func EncodeSnapshot(bitmapData []byte) []byte {
	buf := make([]byte, 2+len(bitmapData))
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubSnapshot
	copy(buf[2:], bitmapData)
	return buf
}

// DecodeSnapshot decodes a full availability snapshot from post-dispatch data.
// Expects data starting from the sub-type byte (no StreamTypeAvailability prefix).
//
//	[1B sub=0x01][roaring64 MarshalBinary bytes]
func DecodeSnapshot(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, errors.New("snapshot: too short")
	}
	if data[0] != AvailSubSnapshot {
		return nil, fmt.Errorf("snapshot: invalid sub-type %02x", data[0])
	}
	return data[1:], nil
}

// EncodeDelta encodes a single availability delta to the wire format:
//
//	[1B type=0x16][1B sub=0x02][1B op][4B pageNo] = 7 bytes
//
// The type prefix is used by the transport layer for dispatch.
func EncodeDelta(op DeltaOp, pageNo uint32) []byte {
	var buf [7]byte
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubDelta
	buf[2] = byte(op)
	binary.BigEndian.PutUint32(buf[3:7], pageNo)
	return buf[:]
}

// DecodeDelta decodes a single availability delta from post-dispatch data.
// Expects data starting from the sub-type byte (no StreamTypeAvailability prefix).
//
//	[1B sub=0x02][1B op][4B pageNo] = 6 bytes
func DecodeDelta(data []byte) (DeltaOp, uint32, error) {
	if len(data) < 6 {
		return 0, 0, errors.New("delta: too short")
	}
	if data[0] != AvailSubDelta {
		return 0, 0, fmt.Errorf("delta: invalid sub-type %02x", data[0])
	}
	op := DeltaOp(data[1])
	pageNo := binary.BigEndian.Uint32(data[2:6])
	return op, pageNo, nil
}
