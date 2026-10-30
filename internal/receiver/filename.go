package receiver

import (
	"path/filepath"
	"strings"
	"unicode"
)

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

	nameRunes := []rune(name)
	if len(nameRunes) > 255 {
		name = string(nameRunes[:255])
	}

	// Base("/") is "/"; anything still holding a separator is not a name.
	if name == "" || strings.ContainsAny(name, "/") || !filepath.IsLocal(name) {
		return fallback
	}
	return name
}
