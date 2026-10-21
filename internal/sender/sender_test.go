package sender

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/protocol"
	"github.com/evgenymng/caravan/internal/receiver"
	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

const chunkSize = transfer.MinChunkSize

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func tempFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), "src.bin")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path, data
}

func startReceiver(t *testing.T, dir string, cfg receiver.Config) string {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	cfg.Dir = dir
	cfg.MaxTransfers = 2
	if cfg.MaxChunkSize == 0 {
		cfg.MaxChunkSize = 1 << 20
	}
	cfg.ReadTimeout = 5 * time.Second
	srv, err := receiver.NewServer(cfg, discard)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv.Addr().String()
}

func opts(path, addr string) Options {
	return Options{
		Path:            path,
		Addr:            addr,
		ChunkSize:       chunkSize,
		MaxRetries:      5,
		RetryDelay:      10 * time.Millisecond,
		DialTimeout:     time.Second,
		AckTimeout:      5 * time.Second,
		FinalizeTimeout: 5 * time.Second,
		DropAfterChunk:  -1,
	}
}

func TestSender_FullTransfer(t *testing.T) {
	dir := t.TempDir()
	addr := startReceiver(t, dir, receiver.Config{})
	path, data := tempFile(t, 3*chunkSize+7)

	s := New(opts(path, addr), discard)
	require.NoError(t, s.Run(context.Background()))

	got, err := os.ReadFile(filepath.Join(dir, "src.bin"))
	require.NoError(t, err)
	require.Equal(t, data, got)
	st := s.Stats()
	require.Equal(t, uint64(4), st.Sent)
	require.Equal(t, uint64(4), st.Acked)
	require.Equal(t, uint64(0), st.Skipped)
	require.Equal(t, uint64(len(data)), st.BytesAcked)
	want := sha256.Sum256(data)
	require.Equal(t, transfer.NewMeta("src.bin", uint64(len(data)), chunkSize,
		hex.EncodeToString(want[:])).TransferID, st.Meta.TransferID)
}

func TestSender_ResumeAfterLostAck(t *testing.T) {
	dir := t.TempDir()
	addr := startReceiver(t, dir, receiver.Config{})
	path, data := tempFile(t, 4*chunkSize)

	o := opts(path, addr)
	o.DropAfterChunk = 1 // close right after sending chunk 1, before its ACK
	s := New(o, discard)
	require.NoError(t, s.Run(context.Background()))

	got, err := os.ReadFile(filepath.Join(dir, "src.bin"))
	require.NoError(t, err)
	require.Equal(t, data, got)
	st := s.Stats()
	require.Equal(t, uint64(4), st.Sent, "chunks 0,1 then 2,3 — no resend")
	require.Equal(t, uint64(3), st.Acked, "ack for chunk 1 was never read")
	require.Equal(t, uint64(2), st.Skipped, "receiver reported 0 and 1")
	require.Equal(t, uint64(0), st.Resent, "no acked chunk sent again")
	require.Equal(t, 1, st.Reconnects)
}

func TestSender_AdoptsReceiverChunkSize(t *testing.T) {
	dir := t.TempDir()
	addr := startReceiver(t, dir, receiver.Config{})
	path, data := tempFile(t, 4*chunkSize)

	o := opts(path, addr)
	o.DropAfterChunk = 0
	o.RetryDelay = time.Hour // only the first attempt runs
	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()
	require.ErrorIs(t, New(o, discard).Run(ctx), context.DeadlineExceeded)

	o = opts(path, addr)
	o.ChunkSize = 2 * chunkSize
	second := New(o, discard)
	require.NoError(t, second.Run(context.Background()))
	require.Equal(t, uint32(chunkSize), second.Stats().Meta.ChunkSize)
	require.Equal(t, uint64(1), second.Stats().Skipped)
	got, err := os.ReadFile(filepath.Join(dir, "src.bin"))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestSender_SourceChanged(t *testing.T) {
	path, _ := tempFile(t, chunkSize)
	s := New(opts(path, "127.0.0.1:1"), discard)
	require.NoError(t, s.open(context.Background()))
	defer s.close()

	require.NoError(t, s.checkSource())
	require.NoError(t, os.WriteFile(path, []byte("changed"), 0o600))
	require.ErrorIs(t, s.checkSource(), ErrSourceChanged)
}

func TestSender_FatalErrorNoRetry(t *testing.T) {
	addr := startReceiver(t, t.TempDir(),
		receiver.Config{MaxChunkSize: chunkSize / 2})
	path, _ := tempFile(t, chunkSize)
	s := New(opts(path, addr), discard)

	err := s.Run(context.Background())
	var perr *protocol.Error
	require.ErrorAs(t, err, &perr)
	require.Equal(t, protocol.CodeInvalidMeta, perr.Code)
	require.Equal(t, 0, s.Stats().Reconnects)
}

func TestSender_GivesUpAfterRetries(t *testing.T) {
	path, _ := tempFile(t, chunkSize)
	o := opts(path, "127.0.0.1:1")
	o.MaxRetries = 2
	s := New(o, discard)
	err := s.Run(context.Background())
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrSourceChanged))
	require.Equal(t, 2, s.Stats().Reconnects)
}

func TestSender_ContextCancel(t *testing.T) {
	path, _ := tempFile(t, chunkSize)
	o := opts(path, "127.0.0.1:1")
	o.MaxRetries = 0
	o.RetryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	require.ErrorIs(t, New(o, discard).Run(ctx), context.Canceled)
}

// fakeReceiver answers the handshake with an arbitrary RESUME_STATE.
func fakeReceiver(t *testing.T, rs protocol.ResumeState) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				var hello protocol.Hello
				_ = protocol.Expect(conn, protocol.TypeHello, 1<<16, &hello)
				_ = protocol.WriteJSON(conn, protocol.TypeHelloOK, hello)
				var meta protocol.Meta
				_ = protocol.Expect(conn, protocol.TypeMeta, 1<<16, &meta)
				rs.TransferID = meta.TransferID
				_ = protocol.WriteJSON(conn, protocol.TypeResumeState, rs)
				_, _, _ = protocol.ReadFrame(conn, 1<<20)
			}()
		}
	}()
	return ln.Addr().String()
}

func TestSender_RejectsBogusResumeState(t *testing.T) {
	path, _ := tempFile(t, 2*chunkSize)
	cases := map[string]protocol.ResumeState{
		"range past end": {
			ChunkSize: chunkSize, Chunks: 2,
			Received: [][2]uint64{{0, 5}},
		},
		"inverted range": {
			ChunkSize: chunkSize, Chunks: 2,
			Received: [][2]uint64{{1, 0}},
		},
		"chunk count mismatch": {ChunkSize: chunkSize, Chunks: 7},
		"tiny chunk size":      {ChunkSize: 1, Chunks: 2 * chunkSize},
	}
	for name, rs := range cases {
		t.Run(name, func(t *testing.T) {
			o := opts(path, fakeReceiver(t, rs))
			o.MaxRetries = 1
			err := New(o, discard).Run(context.Background())
			require.ErrorIs(t, err, ErrBadResumeState)
		})
	}
}
