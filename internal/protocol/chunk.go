package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/evgenymng/caravan/internal/transfer"
)

// chunkHeaderLen is uint64 index + uint32 size + 32-byte SHA-256.
const chunkHeaderLen = 8 + 4 + 32

// Chunk is a decoded CHUNK payload. Data aliases the frame buffer.
type Chunk struct {
	Index uint64
	Sum   [32]byte
	Data  []byte
}

// Verify recomputes the data checksum and compares it with the header.
func (c Chunk) Verify() bool { return transfer.HashBytes(c.Data) == c.Sum }

// WriteChunk writes chunk index with data as a CHUNK frame.
func WriteChunk(w io.Writer, index uint64, data []byte) error {
	if len(data) > math.MaxUint32 {
		return ErrFrameTooLarge
	}
	size := uint32(len(data)) //nolint:gosec // guarded above
	var hdr [chunkHeaderLen]byte
	binary.BigEndian.PutUint64(hdr[:8], index)
	binary.BigEndian.PutUint32(hdr[8:12], size)
	sum := transfer.HashBytes(data)
	copy(hdr[12:], sum[:])
	return writeFrame(w, TypeChunk, hdr[:], data)
}

// DecodeChunk parses a CHUNK payload without verifying the checksum.
func DecodeChunk(payload []byte) (Chunk, error) {
	if len(payload) < chunkHeaderLen {
		return Chunk{}, fmt.Errorf("chunk payload too short: %d", len(payload))
	}
	size := binary.BigEndian.Uint32(payload[8:12])
	if int(size) != len(payload)-chunkHeaderLen {
		return Chunk{}, fmt.Errorf("chunk size %d does not match payload %d",
			size, len(payload)-chunkHeaderLen)
	}
	c := Chunk{
		Index: binary.BigEndian.Uint64(payload[:8]),
		Data:  payload[chunkHeaderLen:],
	}
	copy(c.Sum[:], payload[12:chunkHeaderLen])
	return c, nil
}
