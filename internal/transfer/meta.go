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

// MinChunkSize is the smallest chunk size a receiver accepts.
const MinChunkSize = 4 << 10

// Status values form the transfer lifecycle on the receiver.
const (
	StatusCreated      Status = "created"
	StatusTransferring Status = "transferring"
	StatusInterrupted  Status = "interrupted"
	StatusCompleted    Status = "completed"
	StatusFailed       Status = "failed"
)

var ErrInvalidMeta = errors.New("invalid transfer metadata")

// Status is the lifecycle state of a transfer on the receiver.
type Status string

// Meta describes a file being transferred. It is sent by the sender
// before any chunk and persisted by the receiver.
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
		Chunks:     (size + uint64(chunkSize) - 1) / uint64(chunkSize),
		Checksum:   sum,
	}
}

// ChunkLen returns the byte length of chunk i (the last one may be short).
func (m Meta) ChunkLen(i uint64) uint32 {
	if rest := m.FileSize - i*uint64(m.ChunkSize); rest < uint64(m.ChunkSize) {
		return uint32(rest) //nolint:gosec // rest < ChunkSize fits uint32
	}
	return m.ChunkSize
}

// Validate checks internal consistency against the receiver's limits.
func (m Meta) Validate(maxChunkSize uint32) error {
	switch {
	case m.TransferID == "" || m.FileName == "" || len(m.Checksum) != 64:
		return fmt.Errorf("%w: empty id, name or checksum", ErrInvalidMeta)
	case m.ChunkSize < MinChunkSize || m.ChunkSize > maxChunkSize:
		return fmt.Errorf("%w: chunk size %d outside [%d, %d]",
			ErrInvalidMeta, m.ChunkSize, MinChunkSize, maxChunkSize)
	case m.Chunks != (m.FileSize+uint64(m.ChunkSize)-1)/uint64(m.ChunkSize):
		return fmt.Errorf("%w: %d chunks for %d bytes of %d",
			ErrInvalidMeta, m.Chunks, m.FileSize, m.ChunkSize)
	}
	return nil
}

// TransferID derives a stable id from file checksum, size and name, so the
// same file resumes under the same id without sender-side state.
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

// HashBytes returns the SHA-256 of b.
func HashBytes(b []byte) [32]byte { return sha256.Sum256(b) }
