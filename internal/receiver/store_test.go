package receiver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

const testChunk = transfer.MinChunkSize

func testFile(t *testing.T, size int) ([]byte, transfer.Meta) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	sum := sha256.Sum256(data)
	meta := transfer.NewMeta("f.bin", uint64(size), testChunk,
		hex.EncodeToString(sum[:]))
	return data, meta
}

func chunk(data []byte, m transfer.Meta, i uint64) []byte {
	from := i * uint64(m.ChunkSize)
	return data[from : from+uint64(m.ChunkLen(i))]
}

func newStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := NewStore(dir, true)
	require.NoError(t, err)
	return s
}

func TestStore_FullTransfer(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	data, meta := testFile(t, 2*testChunk+100)

	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	require.Equal(t, transfer.StatusTransferring, tr.Status())
	require.Equal(t, uint64(3), tr.Meta().Chunks)
	st, err := os.Stat(
		filepath.Join(dir, ".caravan", meta.TransferID, "f.bin.part"),
	)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), st.Size())

	for i := range meta.Chunks {
		dup, err := tr.WriteChunk(i, chunk(data, meta, i))
		require.NoError(t, err)
		require.False(t, dup)
	}
	dup, err := tr.WriteChunk(1, chunk(data, meta, 1))
	require.NoError(t, err)
	require.True(t, dup)
	require.Equal(t, [][2]uint64{{0, 2}}, tr.Received())

	path, err := tr.Finalize()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "f.bin"), path)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got))
	state := filepath.Join(dir, ".caravan", meta.TransferID)
	require.NoFileExists(t, filepath.Join(state, "f.bin.part"))
	require.NoFileExists(t, filepath.Join(state, "received.bitmap"))
	require.FileExists(t, filepath.Join(state, "metadata.json"))
	tr.Release(transfer.StatusInterrupted) // no-op after finalize
}

func TestStore_WriteChunkErrors(t *testing.T) {
	s := newStore(t, t.TempDir())
	data, meta := testFile(t, testChunk+1)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	defer tr.Release(transfer.StatusInterrupted)

	_, err = tr.WriteChunk(2, data[:1])
	require.ErrorIs(t, err, ErrOutOfRange)
	_, err = tr.WriteChunk(1, data[:2])
	require.ErrorIs(t, err, ErrBadChunk)
	_, err = tr.Finalize()
	require.ErrorIs(t, err, ErrIncomplete)
}

func TestStore_FinalizeMismatchResets(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	data, meta := testFile(t, testChunk)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	wrong := bytes.Clone(data)
	wrong[0] ^= 0xff
	_, err = tr.WriteChunk(0, wrong)
	require.NoError(t, err)
	_, err = tr.Finalize()
	require.ErrorIs(t, err, ErrChecksumMismatch)
	state := filepath.Join(dir, ".caravan", meta.TransferID)
	require.NoFileExists(t, filepath.Join(state, "f.bin.part"))
	require.NoFileExists(t, filepath.Join(dir, "f.bin"))
	list, err := s.Scan()
	require.NoError(t, err)
	require.Equal(t, transfer.StatusFailed, list[0].Status)

	// Next attempt starts from scratch.
	tr, err = s.Acquire(meta, func() {})
	require.NoError(t, err)
	require.Equal(t, transfer.StatusTransferring, tr.Status())
	tr.Release(transfer.StatusInterrupted)
}

func TestStore_ResumeAfterRestart(t *testing.T) {
	dir := t.TempDir()
	data, meta := testFile(t, 3*testChunk)
	s := newStore(t, dir)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	_, err = tr.WriteChunk(0, chunk(data, meta, 0))
	require.NoError(t, err)
	_, err = tr.WriteChunk(2, chunk(data, meta, 2))
	require.NoError(t, err)
	tr.Release(transfer.StatusInterrupted)

	s2 := newStore(t, dir) // "restart"
	list, err := s2.Scan()
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, transfer.StatusInterrupted, list[0].Status)
	require.Equal(t, uint64(2), list[0].ReceivedChunks)

	// Sender comes back with a different chunk size: stored one wins.
	other := meta
	other.ChunkSize = 2 * testChunk
	other.Chunks = 2
	tr2, err := s2.Acquire(other, func() {})
	require.NoError(t, err)
	require.Equal(t, uint32(testChunk), tr2.Meta().ChunkSize)
	require.Equal(t, [][2]uint64{{0, 0}, {2, 2}}, tr2.Received())
	_, err = tr2.WriteChunk(1, chunk(data, meta, 1))
	require.NoError(t, err)
	path, err := tr2.Finalize()
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got))
}

func TestStore_MetaMismatch(t *testing.T) {
	s := newStore(t, t.TempDir())
	_, meta := testFile(t, testChunk)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	tr.Release(transfer.StatusInterrupted)

	other := meta
	other.Checksum = strings.Repeat("b", 64)
	_, err = s.Acquire(other, func() {})
	require.ErrorIs(t, err, ErrMetaMismatch)
}

func TestStore_InvalidMeta(t *testing.T) {
	s := newStore(t, t.TempDir())
	_, meta := testFile(t, testChunk)
	meta.Chunks = 9
	_, err := s.Acquire(meta, func() {})
	require.ErrorIs(t, err, transfer.ErrInvalidMeta)
}

func TestStore_PreemptsActiveSession(t *testing.T) {
	s := newStore(t, t.TempDir())
	_, meta := testFile(t, testChunk)
	preempted := make(chan struct{})
	tr, err := s.Acquire(meta, func() { close(preempted) })
	require.NoError(t, err)

	go func() {
		<-preempted
		time.Sleep(20 * time.Millisecond)
		tr.Release(transfer.StatusInterrupted)
	}()
	tr2, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	tr2.Release(transfer.StatusInterrupted)
}

func TestStore_PreemptTimeout(t *testing.T) {
	s := newStore(t, t.TempDir())
	s.preemptWait = 50 * time.Millisecond
	_, meta := testFile(t, testChunk)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	defer tr.Release(transfer.StatusInterrupted)

	_, err = s.Acquire(meta, func() {})
	require.ErrorIs(t, err, ErrBusy)
}

func TestStore_NameCollision(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f.bin"), nil, 0o600))
	s := newStore(t, dir)
	data, meta := testFile(t, 10)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	_, err = tr.WriteChunk(0, data)
	require.NoError(t, err)
	path, err := tr.Finalize()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "f (1).bin"), path)
}

func TestStore_ScanIsReadOnlyRecoverMarksInterrupted(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	_, meta := testFile(t, testChunk)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	_ = tr // simulate kill -9: never released

	other := newStore(t, dir) // e.g. `caravan status` while receiver runs
	list, err := other.Scan()
	require.NoError(t, err)
	require.Equal(t, transfer.StatusTransferring, list[0].Status)

	require.NoError(t, other.Recover()) // receiver restart
	list, err = other.Scan()
	require.NoError(t, err)
	require.Equal(t, transfer.StatusInterrupted, list[0].Status)
	raw, err := os.ReadFile(
		filepath.Join(dir, ".caravan", meta.TransferID, "metadata.json"))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"interrupted"`)
}

func TestStore_CompletedIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	data, meta := testFile(t, testChunk+3)
	tr, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	for i := range meta.Chunks {
		_, err = tr.WriteChunk(i, chunk(data, meta, i))
		require.NoError(t, err)
	}
	path, err := tr.Finalize()
	require.NoError(t, err)

	// COMPLETE was lost: sender comes back with the same META.
	again, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	require.Equal(t, transfer.StatusCompleted, again.Status())
	require.Equal(t, [][2]uint64{{0, 1}}, again.Received())
	dup, err := again.WriteChunk(0, chunk(data, meta, 0))
	require.NoError(t, err)
	require.True(t, dup)
	path2, err := again.Finalize()
	require.NoError(t, err)
	require.Equal(t, path, path2)
	require.NoFileExists(t, filepath.Join(dir, "f (1).bin"))

	list, err := s.Scan()
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, transfer.StatusCompleted, list[0].Status)

	// The user deleted the result: start over.
	require.NoError(t, os.Remove(path))
	fresh, err := s.Acquire(meta, func() {})
	require.NoError(t, err)
	require.Equal(t, transfer.StatusTransferring, fresh.Status())
	require.Nil(t, fresh.Received())
	fresh.Release(transfer.StatusInterrupted)
}
