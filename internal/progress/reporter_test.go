package progress

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRender(t *testing.T) {
	line := render("send", 337<<20, 524<<20, 322, 500, 10<<20, 18*time.Second)
	require.Equal(t,
		"send  337.0 MB / 524.0 MB  64.3%  322/500 chunks  10.0 MB/s  ETA 18s",
		line)
	require.Equal(t,
		"recv  0 B / 10 B  0.0%  0/1 chunks  0 B/s  ETA --",
		render("recv", 0, 10, 0, 1, 0, 0))
}

func TestReporter_ThrottlesAndFinishes(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "send", 100, false)
	r.interval = time.Hour
	r.Update(10, 1, 10)
	r.Update(20, 2, 10)
	require.Equal(t, 1, strings.Count(out.String(), "\n"), "first only")
	r.Finish(100, 10)
	require.Equal(t, 2, strings.Count(out.String(), "\n"))
	require.Contains(t, out.String(), "100.0%")
}

func TestReporter_TTYUsesCarriageReturn(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "send", 100, true)
	r.Update(10, 1, 10)
	require.True(t, strings.HasPrefix(out.String(), "\r"))
	require.False(t, strings.HasSuffix(out.String(), "\n"))
	r.Finish(100, 10)
	require.True(t, strings.HasSuffix(out.String(), "\n"))
}
