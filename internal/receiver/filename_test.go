package receiver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"archive.zip":            "archive.zip",
		"../../important.file":   "important.file",
		"/etc/passwd":            "passwd",
		`..\..\win.ini`:          "win.ini",
		"..":                     "id",
		".":                      "id",
		"":                       "id",
		"a\x00b":                 "ab",
		"отчёт.pdf":              "отчёт.pdf",
		".hidden":                "hidden",
		strings.Repeat("x", 300): strings.Repeat("x", 255),
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, want, sanitizeName(in, "id"))
		})
	}
}
