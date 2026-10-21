package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/evgenymng/caravan/internal/receiver"
	"github.com/evgenymng/caravan/internal/transfer"
	"github.com/stretchr/testify/require"
)

func TestStatus_PrintsStoredTransfer(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("x"), 2*transfer.MinChunkSize)
	sum := sha256.Sum256(data)
	meta := transfer.NewMeta("a.bin", uint64(len(data)), transfer.MinChunkSize,
		hex.EncodeToString(sum[:]))
	store, err := receiver.NewStore(dir, false)
	require.NoError(t, err)
	tr, err := store.Acquire(meta, func() {})
	require.NoError(t, err)
	_, err = tr.WriteChunk(0, data[:transfer.MinChunkSize])
	require.NoError(t, err)
	tr.Release(transfer.StatusInterrupted)

	var out bytes.Buffer
	cmd := NewRootCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"status", "--dir", dir})
	require.NoError(t, cmd.Execute())
	for _, want := range []string{
		"transfer: " + meta.TransferID, "file: a.bin", "status: interrupted",
		"1 / 2 chunks", "50.0 %", "4.0 KB / 8.0 KB",
	} {
		require.Contains(t, out.String(), want)
	}

	out.Reset()
	cmd = NewRootCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"status", "--dir", dir, "nope"})
	require.Error(t, cmd.Execute())
}
