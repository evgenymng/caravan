package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/evgenymng/caravan/internal/progress"
)

var sizeUnits = map[string]uint64{
	"": 1, "b": 1,
	"k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10,
	"m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
	"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30,
}

// parseSize parses "1048576", "256KB", "1MB", "4 MiB" into bytes.
func parseSize(s string) (uint64, error) {
	s = strings.ToLower(strings.ReplaceAll(s, " ", ""))
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, err := strconv.ParseUint(s[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	mult, ok := sizeUnits[s[i:]]
	if !ok {
		return 0, fmt.Errorf("invalid size unit %q", s[i:])
	}
	return n * mult, nil
}

func formatBytes(n uint64) string { return progress.FormatBytes(n) }
