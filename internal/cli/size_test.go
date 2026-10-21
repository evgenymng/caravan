package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSize(t *testing.T) {
	cases := map[string]uint64{
		"1048576": 1 << 20,
		"256KB":   256 << 10,
		"256kb":   256 << 10,
		"1MB":     1 << 20,
		"4 MB":    4 << 20,
		"2GB":     2 << 30,
		"1MiB":    1 << 20,
	}
	for in, want := range cases {
		got, err := parseSize(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "MB", "1TB", "-1MB", "1.5MB"} {
		_, err := parseSize(in)
		require.Error(t, err, in)
	}
}

func TestFormatBytes(t *testing.T) {
	require.Equal(t, "512 B", formatBytes(512))
	require.Equal(t, "1.0 KB", formatBytes(1024))
	require.Equal(t, "337.0 MB", formatBytes(337<<20))
	require.Equal(t, "1.5 GB", formatBytes(3<<29))
}
