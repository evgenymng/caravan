package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/receiver"
	"github.com/evgenymng/caravan/internal/sender"
	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

func TestPlainTransfer(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir, Fsync: true})
	path, sum := tempFile(t, "archive.zip", 3<<20)

	s := sender.New(senderOpts(path, rcv.Addr().String(), 256<<10), discard)
	require.NoError(t, s.Run(context.Background()))

	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "archive.zip")))
	require.Equal(t, uint64(12), s.Stats().Acked)
	require.NoFileExists(
		t,
		filepath.Join(dir, ".caravan", s.Stats().Meta.TransferID),
	)
}

func TestHundredMegabytes(t *testing.T) {
	if testing.Short() {
		t.Skip("100 MB transfer skipped in -short mode")
	}
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	path, sum := tempFile(t, "big.bin", 100<<20)

	s := sender.New(senderOpts(path, rcv.Addr().String(), 1<<20), discard)
	require.NoError(t, s.Run(context.Background()))
	require.Equal(t, uint64(100), s.Stats().Meta.Chunks)
	require.Equal(t, uint64(100), s.Stats().Acked)
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "big.bin")))
}

func TestDisconnectAt40Percent(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	px := startProxy(t, rcv.Addr().String())
	px.killAfter = 20
	path, sum := tempFile(t, "f.bin", 50*chunk64K)

	s := sender.New(senderOpts(path, px.Addr(), chunk64K), discard)
	require.NoError(t, s.Run(context.Background()))

	st := s.Stats()
	require.Equal(t, 1, st.Reconnects)
	require.Equal(t, uint64(20), st.Skipped, "receiver kept chunks 0..19")
	require.Equal(t, uint64(0), st.Resent, "no acked chunk sent again")
	// 50, or 51 when chunk 20 hit the socket just before the proxy died.
	require.LessOrEqual(t, st.Sent, uint64(51))
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "f.bin")))
}

func TestLostAckIsNotResent(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	path, sum := tempFile(t, "f.bin", 10*chunk64K)

	o := senderOpts(path, rcv.Addr().String(), chunk64K)
	o.DropAfterChunk = 4 // receiver stores chunk 4, sender never sees its ACK
	s := sender.New(o, discard)
	require.NoError(t, s.Run(context.Background()))

	st := s.Stats()
	require.Equal(t, uint64(10), st.Sent)
	require.Equal(t, uint64(9), st.Acked)
	require.Equal(t, uint64(5), st.Skipped, "0..4 reported by receiver")
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "f.bin")))
}

func TestCorruptedChunkIsRejectedAndResent(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	px := startProxy(t, rcv.Addr().String())
	px.corruptChunk = 3
	path, sum := tempFile(t, "f.bin", 5*chunk64K)

	s := sender.New(senderOpts(path, px.Addr(), chunk64K), discard)
	require.NoError(t, s.Run(context.Background()))

	require.Equal(t, uint64(6), s.Stats().Sent, "one chunk sent twice")
	require.Equal(t, uint64(5), s.Stats().Acked)
	require.Equal(t, 0, s.Stats().Reconnects)
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "f.bin")))
}

func TestModifiedSourceIsRefused(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	px := startProxy(t, rcv.Addr().String())
	px.killAfter = 3
	path, _ := tempFile(t, "f.bin", 10*chunk64K)

	o := senderOpts(path, px.Addr(), chunk64K)
	o.RetryDelay = 500 * time.Millisecond
	go func() {
		<-px.killed
		_ = os.WriteFile(path, []byte("rewritten"), 0o600)
	}()
	err := sender.New(o, discard).Run(context.Background())
	require.ErrorIs(t, err, sender.ErrSourceChanged)
	require.NoFileExists(t, filepath.Join(dir, "f.bin"))

	// The rewritten file is a new transfer with its own id.
	s2 := sender.New(senderOpts(path, rcv.Addr().String(), chunk64K), discard)
	require.NoError(t, s2.Run(context.Background()))
	require.Equal(t, uint64(0), s2.Stats().Skipped)
	require.Equal(t, fileSum(t, path), fileSum(t, filepath.Join(dir, "f.bin")))
}

func TestReceiverRestartResumesFromDisk(t *testing.T) {
	dir := t.TempDir()
	var first *testReceiver
	stopOnce := make(chan struct{}, 1)
	first = startReceiver(t, receiver.Config{
		Dir: dir, Fsync: true,
		OnChunk: func(_ transfer.Meta, received uint64) {
			if received == 30 {
				select {
				case stopOnce <- struct{}{}:
					first.cancel()
				default:
				}
			}
		},
	})
	addr := first.Addr().String()
	path, sum := tempFile(t, "f.bin", 60*chunk64K)

	done := make(chan *sender.Sender, 1)
	go func() {
		s := sender.New(senderOpts(path, addr, chunk64K), discard)
		_ = s.Run(context.Background())
		done <- s
	}()

	<-stopOnce
	require.NoError(t, first.wait())
	second := startReceiver(
		t,
		receiver.Config{Dir: dir, Addr: addr, Fsync: true},
	)
	_ = second

	s := <-done
	st := s.Stats()
	require.GreaterOrEqual(t, st.Skipped, uint64(30))
	require.Equal(t, uint64(0), st.Resent, "no acked chunk sent again")
	require.LessOrEqual(t, st.Sent, uint64(61))
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "f.bin")))
}

func TestFlakyLinkDoesNotExhaustRetries(t *testing.T) {
	dir := t.TempDir()
	rcv := startReceiver(t, receiver.Config{Dir: dir})
	px := startProxy(t, rcv.Addr().String())
	px.killEvery = 3 // every connection dies after three chunks
	path, sum := tempFile(t, "f.bin", 12*chunk64K)

	o := senderOpts(path, px.Addr(), chunk64K)
	o.MaxRetries = 1 // one retry per outage is enough when each one made progress
	s := sender.New(o, discard)
	require.NoError(t, s.Run(context.Background()))
	require.GreaterOrEqual(t, s.Stats().Reconnects, 3)
	require.Equal(t, uint64(0), s.Stats().Resent)
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "f.bin")))
}
