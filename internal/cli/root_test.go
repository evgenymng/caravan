package cli

import (
	"bytes"
	"testing"

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

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		ok   bool
		want string
	}{
		{"debug", true, "DEBUG"},
		{"INFO", true, "INFO"},
		{"warn", true, "WARN"},
		{"error", true, "ERROR"},
		{"loud", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			lvl, err := parseLevel(tc.in)
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, lvl.String())
		})
	}
}

func TestSend_RejectsTinyChunkSize(t *testing.T) {
	cmd := NewRootCmd()
	cmd.SetArgs([]string{
		"send", "--file", "x.bin", "--addr", "127.0.0.1:1",
		"--chunk-size", "0",
	})
	require.ErrorContains(t, cmd.Execute(), "--chunk-size")
}
