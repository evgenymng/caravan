// Package protocol implements the caravan wire format: length-prefixed
// frames carrying either JSON control messages or binary chunk payloads.
//
// Frame layout: uint32 BE length (type + payload) · uint8 type · payload.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
)

// Version is the protocol version exchanged in HELLO.
const Version = 1

const (
	TypeHello Type = iota + 1
	TypeHelloOK
	TypeMeta
	TypeResumeState
	TypeChunk
	TypeAck
	TypeFinalize
	TypeComplete
	TypeError
)

const frameHeaderLen = 5

var (
	ErrFrameTooLarge  = errors.New("frame too large")
	ErrUnexpectedType = errors.New("unexpected message type")
)

// Type identifies the message carried by a frame.
type Type uint8

// WriteFrame writes one frame with the given type and payload.
func WriteFrame(w io.Writer, t Type, payload []byte) error {
	return writeFrame(w, t, payload, nil)
}

// writeFrame writes a frame whose payload is head followed by tail, using
// a single vectored write so large chunks are never copied.
func writeFrame(w io.Writer, t Type, head, tail []byte) error {
	var hdr [frameHeaderLen]byte
	n := 1 + len(head) + len(tail)
	if n > 1<<31 {
		return ErrFrameTooLarge
	}
	binary.BigEndian.PutUint32(hdr[:], uint32(n)) //nolint:gosec // checked
	hdr[4] = byte(t)
	bufs := net.Buffers{hdr[:], head, tail}
	_, err := bufs.WriteTo(w)
	return err
}

// ReadFrame reads one frame, rejecting payloads larger than maxPayload.
func ReadFrame(r io.Reader, maxPayload uint32) (Type, []byte, error) {
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n-1 > maxPayload {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	payload := make([]byte, n-1)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return Type(hdr[4]), payload, nil
}

// WriteJSON writes v as a JSON control frame of type t.
func WriteJSON(w io.Writer, t Type, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return WriteFrame(w, t, b)
}

// Expect reads the next frame, which must be of type want, and decodes
// its JSON payload into v. An ERROR frame is returned as *Error.
func Expect(r io.Reader, want Type, maxPayload uint32, v any) error {
	t, payload, err := ReadFrame(r, maxPayload)
	if err != nil {
		return err
	}
	if t == TypeError {
		var e Error
		if err := json.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("decode error frame: %w", err)
		}
		return &e
	}
	if t != want {
		return fmt.Errorf("%w: got %d, want %d", ErrUnexpectedType, t, want)
	}
	return json.Unmarshal(payload, v)
}
