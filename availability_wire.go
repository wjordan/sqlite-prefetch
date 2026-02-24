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
//	[1B type=0x13][1B sub=0x01][roaring64 MarshalBinary bytes]
func EncodeSnapshot(bitmapData []byte) []byte {
	buf := make([]byte, 2+len(bitmapData))
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubSnapshot
	copy(buf[2:], bitmapData)
	return buf
}

// DecodeSnapshot decodes a full availability snapshot from the wire format.
// Returns the raw roaring64 bitmap bytes.
func DecodeSnapshot(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return nil, errors.New("snapshot: too short")
	}
	if data[0] != StreamTypeAvailability || data[1] != AvailSubSnapshot {
		return nil, fmt.Errorf("snapshot: invalid header %02x %02x", data[0], data[1])
	}
	return data[2:], nil
}

// EncodeDelta encodes a single availability delta to the wire format:
//
//	[1B type=0x13][1B sub=0x02][1B op][8B logicalAddr] = 11 bytes
func EncodeDelta(op DeltaOp, logicalAddr uint64) []byte {
	var buf [11]byte
	buf[0] = StreamTypeAvailability
	buf[1] = AvailSubDelta
	buf[2] = byte(op)
	binary.BigEndian.PutUint64(buf[3:11], logicalAddr)
	return buf[:]
}

// DecodeDelta decodes a single availability delta from the wire format.
func DecodeDelta(data []byte) (DeltaOp, uint64, error) {
	if len(data) < 11 {
		return 0, 0, errors.New("delta: too short")
	}
	if data[0] != StreamTypeAvailability || data[1] != AvailSubDelta {
		return 0, 0, fmt.Errorf("delta: invalid header %02x %02x", data[0], data[1])
	}
	op := DeltaOp(data[2])
	logicalAddr := binary.BigEndian.Uint64(data[3:11])
	return op, logicalAddr, nil
}
