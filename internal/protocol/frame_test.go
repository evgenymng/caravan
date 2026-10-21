package protocol

import (
	"bytes"
	"io"
	"testing"

	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

func TestFrame_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, TypeAck, []byte(`{"index":1}`)))
	require.Equal(t, []byte{0, 0, 0, 12, byte(TypeAck)}, buf.Bytes()[:5])

	typ, payload, err := ReadFrame(&buf, 64)
	require.NoError(t, err)
	require.Equal(t, TypeAck, typ)
	require.Equal(t, `{"index":1}`, string(payload))
}

func TestFrame_TooLarge(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, TypeAck, make([]byte, 100)))
	_, _, err := ReadFrame(&buf, 64)
	require.ErrorIs(t, err, ErrFrameTooLarge)
}

func TestFrame_Truncated(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, TypeAck, []byte("hello")))
	short := bytes.NewReader(buf.Bytes()[:7])
	_, _, err := ReadFrame(short, 64)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestJSON_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := ResumeState{TransferID: "abc", Received: [][2]uint64{{0, 3}}}
	require.NoError(t, WriteJSON(&buf, TypeResumeState, in))

	var out ResumeState
	require.NoError(t, Expect(&buf, TypeResumeState, 1<<10, &out))
	require.Equal(t, in, out)
}

func TestExpect_ErrorFrame(t *testing.T) {
	var buf bytes.Buffer
	idx := uint64(7)
	require.NoError(t, WriteJSON(&buf, TypeError,
		Error{Code: CodeBadChecksum, Message: "boom", Index: &idx}))

	var ack Ack
	err := Expect(&buf, TypeAck, 1<<10, &ack)
	var perr *Error
	require.ErrorAs(t, err, &perr)
	require.Equal(t, CodeBadChecksum, perr.Code)
	require.Equal(t, uint64(7), *perr.Index)
	require.Contains(t, err.Error(), "boom")
}

func TestExpect_WrongType(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteJSON(&buf, TypeHello, Hello{Version: 1}))
	err := Expect(&buf, TypeAck, 1<<10, &Ack{})
	require.ErrorIs(t, err, ErrUnexpectedType)
}

func TestChunk_RoundTrip(t *testing.T) {
	data := []byte("chunk data")
	var buf bytes.Buffer
	require.NoError(t, WriteChunk(&buf, 42, data))

	typ, payload, err := ReadFrame(&buf, 1<<10)
	require.NoError(t, err)
	require.Equal(t, TypeChunk, typ)

	c, err := DecodeChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(42), c.Index)
	require.Equal(t, data, c.Data)
	require.Equal(t, transfer.HashBytes(data), c.Sum)
	require.True(t, c.Verify())

	c.Data[0] ^= 0xff
	require.False(t, c.Verify())

	_, err = DecodeChunk(payload[:10])
	require.Error(t, err)
	payload[8+3]++ // declared size no longer matches data length
	_, err = DecodeChunk(payload)
	require.Error(t, err)
}
