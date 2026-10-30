package cli

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRoot_ListsSubcommands(t *testing.T) {
	cmd := NewRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})

	require.NoError(t, cmd.Execute())
	for _, name := range []string{"send", "receive", "status"} {
		require.Contains(t, out.String(), name)
	}
}

func TestRoot_RejectsUnknownLogLevel(t *testing.T) {
	cmd := NewRootCmd()
	cmd.SetArgs([]string{"--log-level", "loud", "status", "--dir", t.TempDir()})
	require.ErrorContains(t, cmd.Execute(), "loud")
}

func TestSend_RejectsTinyChunkSize(t *testing.T) {
	cmd := NewRootCmd()
	cmd.SetArgs([]string{
		"send", "--file", "x.bin", "--addr", "127.0.0.1:1",
		"--chunk-size", "0",
	})
	require.ErrorContains(t, cmd.Execute(), "--chunk-size")
}

func TestReceive_RejectsTinyMaxChunkSize(t *testing.T) {
	cmd := NewRootCmd()
	cmd.SetArgs([]string{
		"receive", "--addr", "127.0.0.1:0", "--dir", t.TempDir(),
		"--max-chunk-size", "1KB",
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorContains(t, cmd.ExecuteContext(ctx), "--max-chunk-size")
}
