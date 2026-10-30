package receiver

import "errors"

var (
	ErrBusy             = errors.New("transfer is busy")
	ErrOutOfRange       = errors.New("chunk index out of range")
	ErrBadChunk         = errors.New("chunk length mismatch")
	ErrIncomplete       = errors.New("transfer incomplete")
	ErrChecksumMismatch = errors.New("file checksum mismatch")
)
