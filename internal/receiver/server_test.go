package receiver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/protocol"
	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

const maxPayload = 1 << 20

// corruptingWriter flips the last byte of the chunk data (the only write
// longer than the headers) so it no longer matches its checksum.
type corruptingWriter struct{ net.Conn }

func (w corruptingWriter) Write(p []byte) (int, error) {
	if len(p) <= 44 {
		return w.Conn.Write(p)
	}
	c := append([]byte(nil), p...)
	c[len(c)-1] ^= 0xff
	return w.Conn.Write(c)
}

func startServer(
	t *testing.T,
	cfg Config,
) (*Server, context.CancelFunc, func() error) {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	if cfg.MaxTransfers == 0 {
		cfg.MaxTransfers = 4
	}
	if cfg.MaxChunkSize == 0 {
		cfg.MaxChunkSize = 1 << 20
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	cfg.Addr = "127.0.0.1:0"
	srv, err := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	wait := sync.OnceValue(func() error { return <-done })
	t.Cleanup(func() { cancel(); _ = wait() })
	return srv, cancel, wait
}

func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// handshake runs HELLO/META and returns the receiver's resume state.
func handshake(
	t *testing.T,
	conn net.Conn,
	meta transfer.Meta,
) protocol.ResumeState {
	t.Helper()
	require.NoError(t, protocol.WriteJSON(conn, protocol.TypeHello,
		protocol.Hello{Version: protocol.Version}))
	var ok protocol.Hello
	require.NoError(
		t,
		protocol.Expect(conn, protocol.TypeHelloOK, maxPayload, &ok),
	)
	require.NoError(t, protocol.WriteJSON(conn, protocol.TypeMeta, meta))
	var rs protocol.ResumeState
	require.NoError(
		t,
		protocol.Expect(conn, protocol.TypeResumeState, maxPayload, &rs),
	)
	return rs
}

func expectError(t *testing.T, conn net.Conn, code string, index ...uint64) {
	t.Helper()
	err := protocol.Expect(conn, protocol.TypeAck, maxPayload, &protocol.Ack{})
	var perr *protocol.Error
	require.ErrorAs(t, err, &perr)
	require.Equal(t, code, perr.Code)
	if len(index) > 0 {
		require.NotNil(t, perr.Index)
		require.Equal(t, index[0], *perr.Index)
	}
}

func TestServer_FullTransfer(t *testing.T) {
	dir := t.TempDir()
	srv, _, _ := startServer(t, Config{Dir: dir})
	data, meta := testFile(t, 2*testChunk+5)
	conn := dial(t, srv)

	rs := handshake(t, conn, meta)
	require.Equal(t, meta.TransferID, rs.TransferID)
	require.Empty(t, rs.Received)
	require.Equal(t, transfer.StatusTransferring, rs.Status)

	for i := range meta.Chunks {
		require.NoError(t, protocol.WriteChunk(conn, i, chunk(data, meta, i)))
		var ack protocol.Ack
		require.NoError(
			t,
			protocol.Expect(conn, protocol.TypeAck, maxPayload, &ack),
		)
		require.Equal(t, i, ack.Index)
	}
	require.NoError(t, protocol.WriteJSON(conn, protocol.TypeFinalize,
		protocol.Finalize{TransferID: meta.TransferID}))
	var done protocol.Complete
	require.NoError(
		t,
		protocol.Expect(conn, protocol.TypeComplete, maxPayload, &done),
	)
	require.Equal(t, meta.Checksum, done.Checksum)

	got, err := os.ReadFile(filepath.Join(dir, "f.bin"))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestServer_BadVersion(t *testing.T) {
	srv, _, _ := startServer(t, Config{})
	conn := dial(t, srv)
	require.NoError(t, protocol.WriteJSON(conn, protocol.TypeHello,
		protocol.Hello{Version: 99}))
	expectError(t, conn, protocol.CodeBadVersion)
}

func TestServer_BusyWhenFull(t *testing.T) {
	srv, _, _ := startServer(t, Config{MaxTransfers: 1})
	_, meta := testFile(t, testChunk)
	first := dial(t, srv)
	handshake(t, first, meta)

	second := dial(t, srv)
	require.NoError(t, protocol.WriteJSON(second, protocol.TypeHello,
		protocol.Hello{Version: protocol.Version}))
	expectError(t, second, protocol.CodeBusy)
}

func TestServer_BadChecksumNotStored(t *testing.T) {
	srv, _, _ := startServer(t, Config{})
	data, meta := testFile(t, 2*testChunk)
	conn := dial(t, srv)
	handshake(t, conn, meta)

	bad := append([]byte(nil), chunk(data, meta, 0)...)
	// Correct header for the real data, then corrupt the bytes on the wire.
	require.NoError(t, protocol.WriteChunk(corruptingWriter{conn}, 0, bad))
	expectError(t, conn, protocol.CodeBadChecksum, 0)

	// Wrong length but right checksum is also rejected.
	require.NoError(t, protocol.WriteChunk(conn, 1, data[:10]))
	expectError(t, conn, protocol.CodeBadChecksum)

	require.NoError(t, protocol.WriteChunk(conn, 0, chunk(data, meta, 0)))
	var ack protocol.Ack
	require.NoError(
		t,
		protocol.Expect(conn, protocol.TypeAck, maxPayload, &ack),
	)
	_ = conn.Close()

	rs := handshake(t, dial(t, srv), meta)
	require.Equal(t, [][2]uint64{{0, 0}}, rs.Received)
}

func TestServer_OutOfRangeCloses(t *testing.T) {
	srv, _, _ := startServer(t, Config{})
	data, meta := testFile(t, testChunk)
	conn := dial(t, srv)
	handshake(t, conn, meta)
	require.NoError(t, protocol.WriteChunk(conn, 7, data))
	expectError(t, conn, protocol.CodeOutOfRange)
	_, _, err := protocol.ReadFrame(conn, maxPayload)
	require.ErrorIs(t, err, io.EOF)
}

func TestServer_IncompleteFinalize(t *testing.T) {
	srv, _, _ := startServer(t, Config{})
	_, meta := testFile(t, testChunk)
	conn := dial(t, srv)
	handshake(t, conn, meta)
	require.NoError(t, protocol.WriteJSON(conn, protocol.TypeFinalize,
		protocol.Finalize{TransferID: meta.TransferID}))
	expectError(t, conn, protocol.CodeIncomplete)
}

func TestServer_IdleTimeout(t *testing.T) {
	srv, _, _ := startServer(t, Config{ReadTimeout: 100 * time.Millisecond})
	conn := dial(t, srv)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, _, err := protocol.ReadFrame(conn, maxPayload)
	require.ErrorIs(t, err, io.EOF)
}

func TestServer_ShutdownMarksInterrupted(t *testing.T) {
	dir := t.TempDir()
	srv, cancel, done := startServer(t, Config{Dir: dir})
	data, meta := testFile(t, 2*testChunk)
	conn := dial(t, srv)
	handshake(t, conn, meta)
	require.NoError(t, protocol.WriteChunk(conn, 0, chunk(data, meta, 0)))
	require.NoError(
		t,
		protocol.Expect(conn, protocol.TypeAck, maxPayload, &protocol.Ack{}),
	)

	cancel()
	require.NoError(t, done())
	md, err := readMeta(filepath.Join(dir, ".caravan", meta.TransferID))
	require.NoError(t, err)
	require.Equal(t, transfer.StatusInterrupted, md.Status)
}

func TestServer_RejectsHugeControlFrame(t *testing.T) {
	srv, _, _ := startServer(t, Config{MaxChunkSize: 64 << 20})
	conn := dial(t, srv)
	// A HELLO claiming a 1 MiB payload must be dropped before allocation.
	hdr := []byte{0, 16, 0, 1, byte(protocol.TypeHello)}
	_, err := conn.Write(hdr)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, _, err = protocol.ReadFrame(conn, maxPayload)
	require.ErrorIs(t, err, io.EOF)
}
