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
	require.Equal(t, "send  337.0 MB / 524.0 MB  64.3%  322/500 chunks  10.0 MB/s  ETA 18s", line)
	require.Equal(t, "recv  0 B / 10 B  0.0%  0/1 chunks  0 B/s  ETA --", render("recv", 0, 10, 0, 1, 0, 0))
}

func TestReporter_ThrottlesAndFinishes(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "send", 100, 0, false)
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
	r := New(&out, "send", 100, 0, true)
	r.Update(10, 1, 10)
	require.True(t, strings.HasPrefix(out.String(), "\r"))
	require.False(t, strings.HasSuffix(out.String(), "\n"))
	r.Finish(100, 10)
	require.True(t, strings.HasSuffix(out.String(), "\n"))
}

func TestReporter_SpeedSkipsResumedBytes(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "recv", 100<<20, 90<<20, false)
	r.start = time.Now().Add(-2 * time.Second)
	r.Update(92<<20, 92, 100)
	require.Contains(t, out.String(), "ETA 8s", "1 MB/s over this run, not 46 MB/s counting resumed bytes")
}

func TestReporter_SpeedRestartsAfterIdle(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "recv", 100<<20, 0, false)
	r.interval = 0
	r.start = time.Now().Add(-10 * time.Minute)
	r.seen = r.start
	r.Update(10<<20, 10, 100)
	r.start = time.Now().Add(-2 * time.Second)
	r.Update(12<<20, 12, 100)
	require.Contains(t, out.String(), "ETA 1m28s", "1 MB/s since the pause, not averaged over it")
}

func TestReporter_FinishUnderSecondShowsSpeed(t *testing.T) {
	var out bytes.Buffer
	r := New(&out, "send", 10<<20, 0, false)
	r.start = time.Now().Add(-500 * time.Millisecond)
	r.Finish(10<<20, 10)
	require.NotContains(t, out.String(), " 0 B/s", "final line averages over the whole run")
}

func TestFormatBytes(t *testing.T) {
	require.Equal(t, "512 B", FormatBytes(512))
	require.Equal(t, "1.0 KB", FormatBytes(1024))
	require.Equal(t, "337.0 MB", FormatBytes(337<<20))
	require.Equal(t, "1.5 GB", FormatBytes(3<<29))
}
