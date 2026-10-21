package tests

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/cli"
	"github.com/stretchr/testify/require"
)

func TestCLI_SendReceive(t *testing.T) {
	dir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	recvDone := make(chan error, 1)
	go func() {
		cmd := cli.NewRootCmd()
		cmd.SetArgs([]string{
			"receive", "--addr", addr, "--dir", dir,
			"--log-level", "error",
		})
		recvDone <- cmd.ExecuteContext(ctx)
	}()
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)

	path, sum := tempFile(t, "doc.pdf", 5*chunk64K+1)
	var out bytes.Buffer
	send := cli.NewRootCmd()
	send.SetOut(&out)
	send.SetArgs([]string{
		"send", "--file", path, "--addr", addr,
		"--chunk-size", "64KB", "--log-level", "error",
	})
	require.NoError(t, send.Execute())
	require.Contains(t, out.String(), "completed: transfer")
	require.Contains(t, out.String(), "100.0%")
	require.Equal(t, sum, fileSum(t, filepath.Join(dir, "doc.pdf")))

	cancel()
	require.NoError(t, <-recvDone)
}
