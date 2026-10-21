package protocol

import (
	"fmt"

	"github.com/evgenymng/caravan/internal/transfer"
)

// Error codes carried by ERROR frames.
const (
	CodeBadVersion       = "bad_version"
	CodeBusy             = "busy"
	CodeInvalidMeta      = "invalid_meta"
	CodeMetaMismatch     = "meta_mismatch"
	CodeBadChecksum      = "bad_checksum"
	CodeOutOfRange       = "out_of_range"
	CodeIncomplete       = "incomplete"
	CodeChecksumMismatch = "checksum_mismatch"
	CodeIOError          = "io_error"
)

// Hello opens a session; HELLO_OK echoes the agreed version.
type Hello struct {
	Version int `json:"version"`
}

// Meta is the META payload: the file description from transfer.Meta.
type Meta = transfer.Meta

// ResumeState is the receiver's answer to META: which chunks it has.
// ChunkSize/Chunks echo the stored values so a sender can adopt them.
type ResumeState struct {
	TransferID     string          `json:"transferId"`
	Status         transfer.Status `json:"status"`
	ChunkSize      uint32          `json:"chunkSize"`
	Chunks         uint64          `json:"chunks"`
	Received       [][2]uint64     `json:"received"`
	ReceivedChunks uint64          `json:"receivedChunks"`
}

// Ack confirms a chunk was verified and stored.
type Ack struct {
	Index uint64 `json:"index"`
}

// Finalize asks the receiver to verify the whole file.
type Finalize struct {
	TransferID string `json:"transferId"`
}

// Complete reports a successful whole-file verification.
type Complete struct {
	TransferID string `json:"transferId"`
	Checksum   string `json:"checksum"`
}

// Error is the ERROR payload; it also implements the error interface.
type Error struct {
	Code    string  `json:"code"`
	Message string  `json:"message"`
	Index   *uint64 `json:"index,omitempty"`
}

func (e *Error) Error() string {
	if e.Index != nil {
		return fmt.Sprintf("%s (chunk %d): %s", e.Code, *e.Index, e.Message)
	}
	return e.Code + ": " + e.Message
}

// Fatal reports whether the error cannot be fixed by reconnecting.
func (e *Error) Fatal() bool {
	switch e.Code {
	case CodeBadVersion, CodeInvalidMeta, CodeMetaMismatch,
		CodeOutOfRange, CodeChecksumMismatch:
		return true
	}
	return false
}

// WriteError sends an ERROR frame.
func WriteError(w interface{ Write([]byte) (int, error) },
	code, msg string, index *uint64,
) error {
	return WriteJSON(
		w,
		TypeError,
		Error{Code: code, Message: msg, Index: index},
	)
}
