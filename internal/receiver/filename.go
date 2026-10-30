package receiver

import (
	"path/filepath"
	"strings"
	"unicode"
)

// maxName leaves room for ".part" and " (n)" within the usual 255-byte file name limit.
const maxName = 240

// sanitizeName drops path components, control characters and leading dots; empty becomes fallback.
func sanitizeName(name, fallback string) string {
	name = strings.ReplaceAll(name, `\`, "/")
	name = filepath.Base(name)
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, name)
	name = strings.TrimLeft(name, ".")

	if len(name) > maxName {
		ext := filepath.Ext(name)
		if len(ext) >= maxName {
			ext = "" // the long part is the extension itself
		}
		name = strings.ToValidUTF8(name[:maxName-len(ext)], "") + ext
	}

	if name == "" || !filepath.IsLocal(name) {
		return fallback
	}
	return name
}
