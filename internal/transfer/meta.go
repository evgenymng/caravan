// Package transfer holds the data model shared by sender and receiver:
// file metadata, transfer identity, chunk bitmap and hashing.
package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const (
	// MinChunkSize is the smallest chunk size a receiver accepts.
	MinChunkSize = 4 << 10
	// MaxChunks bounds the bitmap a receiver allocates per transfer (8 MB).
	MaxChunks = 1 << 26
)

// Status values form the transfer lifecycle on the receiver.
const (
	StatusTransferring Status = "transferring"
	StatusInterrupted  Status = "interrupted"
	StatusCompleted    Status = "completed"
	StatusFailed       Status = "failed"
)

var ErrInvalidMeta = errors.New("invalid transfer metadata")

// Status is the lifecycle state of a transfer on the receiver.
type Status string

// Meta describes the file: the sender sends it before any chunk, the receiver persists it.
type Meta struct {
	TransferID string `json:"transferId"`
	FileName   string `json:"fileName"`
	FileSize   uint64 `json:"fileSize"`
	ChunkSize  uint32 `json:"chunkSize"`
	Chunks     uint64 `json:"chunks"`
	Checksum   string `json:"checksum"`
}

// NewMeta derives chunk count and transfer ID from the file facts.
func NewMeta(name string, size uint64, chunkSize uint32, sum string) Meta {
	return Meta{
		TransferID: TransferID(sum, size, name),
		FileName:   name,
		FileSize:   size,
		ChunkSize:  chunkSize,
		Chunks:     ChunkCount(size, chunkSize),
		Checksum:   sum,
	}
}

// ChunkCount returns how many chunks of chunkSize cover size bytes.
func ChunkCount(size uint64, chunkSize uint32) uint64 {
	return (size + uint64(chunkSize) - 1) / uint64(chunkSize)
}

// ChunkLen returns the byte length of chunk i (the last one may be short).
func (m Meta) ChunkLen(i uint64) uint32 {
	if rest := m.FileSize - i*uint64(m.ChunkSize); rest < uint64(m.ChunkSize) {
		return uint32(rest) //nolint:gosec // rest < ChunkSize fits uint32
	}
	return m.ChunkSize
}

// Validate checks that the fields agree with each other.
func (m Meta) Validate() error {
	switch {
	case m.TransferID == "" || m.FileName == "" || len(m.Checksum) != 64:
		return fmt.Errorf("%w: empty id, name or checksum", ErrInvalidMeta)
	case m.TransferID != TransferID(m.Checksum, m.FileSize, m.FileName):
		return fmt.Errorf("%w: transfer id does not match", ErrInvalidMeta)
	case m.ChunkSize < MinChunkSize:
		return fmt.Errorf("%w: chunk size %d below %d", ErrInvalidMeta, m.ChunkSize, MinChunkSize)
	case m.Chunks != ChunkCount(m.FileSize, m.ChunkSize):
		return fmt.Errorf("%w: %d chunks for %d bytes", ErrInvalidMeta, m.Chunks, m.FileSize)
	case m.Chunks > MaxChunks:
		return fmt.Errorf("%w: %d chunks, max %d", ErrInvalidMeta, m.Chunks, MaxChunks)
	}
	return nil
}

// TransferID derives a stable id from file checksum, size and name, so resume needs no sender state.
func TransferID(sum string, size uint64, name string) string {
	h := sha256.New()
	h.Write([]byte(sum))
	_ = binary.Write(h, binary.BigEndian, size)
	h.Write([]byte(name))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// HashReader streams r through SHA-256 and returns the hex digest.
func HashReader(ctx context.Context, r io.Reader) (string, error) {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := r.Read(buf)
		h.Write(buf[:n])
		if err == io.EOF {
			return hex.EncodeToString(h.Sum(nil)), nil
		}
		if err != nil {
			return "", err
		}
	}
}
