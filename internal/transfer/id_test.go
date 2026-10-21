package transfer

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashReader(t *testing.T) {
	sum, err := HashReader(context.Background(), bytes.NewReader([]byte("abc")))
	require.NoError(t, err)
	require.Equal(t,
		"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		sum)
}

func TestTransferID_Deterministic(t *testing.T) {
	a := TransferID("sum", 10, "a.bin")
	require.Len(t, a, 16)
	require.Equal(t, a, TransferID("sum", 10, "a.bin"))
	require.NotEqual(t, a, TransferID("sum2", 10, "a.bin"))
	require.NotEqual(t, a, TransferID("sum", 11, "a.bin"))
	require.NotEqual(t, a, TransferID("sum", 10, "b.bin"))
}

func TestNewMeta(t *testing.T) {
	sum := strings.Repeat("a", 64)
	m := NewMeta("a.bin", 10000, 4096, sum)
	require.Equal(t, uint64(3), m.Chunks)
	require.Equal(t, uint32(4096), m.ChunkLen(0))
	require.Equal(t, uint32(1808), m.ChunkLen(2))
	require.NoError(t, m.Validate(1<<20))

	cases := map[string]func(*Meta){
		"chunks":    func(m *Meta) { m.Chunks = 5 },
		"too small": func(m *Meta) { m.ChunkSize = 2 },
		"too big":   func(m *Meta) { m.ChunkSize = 2 << 20 },
		"name":      func(m *Meta) { m.FileName = "" },
		"checksum":  func(m *Meta) { m.Checksum = "sum" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := m
			mutate(&bad)
			require.ErrorIs(t, bad.Validate(1<<20), ErrInvalidMeta)
		})
	}
}
