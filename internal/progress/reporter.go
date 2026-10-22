// Package progress renders a one-line transfer progress indicator.
package progress

import (
	"fmt"
	"io"
	"time"
)

// Reporter prints progress to out: a redrawn line on a TTY, or one line
// every interval otherwise.
type Reporter struct {
	out      io.Writer
	label    string
	total    uint64
	tty      bool
	interval time.Duration
	start    time.Time
	last     time.Time
}

// New returns a reporter for total bytes. On a TTY it refreshes 5×/s,
// otherwise it prints a line every 2 s.
func New(out io.Writer, label string, total uint64, tty bool) *Reporter {
	r := &Reporter{
		out: out, label: label, total: total, tty: tty,
		interval: 2 * time.Second, start: time.Now(),
	}
	if tty {
		r.interval = 200 * time.Millisecond
	}
	return r
}

// Update reports done bytes and chunks; it draws at most once per interval.
func (r *Reporter) Update(done, chunksDone, chunksTotal uint64) {
	if now := time.Now(); now.Sub(r.last) >= r.interval {
		r.last = now
		r.draw(done, chunksDone, chunksTotal, false)
	}
}

// Finish draws the final state and ends the line.
func (r *Reporter) Finish(done, chunks uint64) {
	r.draw(done, chunks, chunks, true)
}

// FormatBytes renders n in B/KB/MB/GB with one decimal above bytes.
func FormatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit && exp < 3; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGT"[exp])
}

func (r *Reporter) draw(done, chunksDone, chunksTotal uint64, final bool) {
	elapsed := time.Since(r.start).Seconds()
	var speed, eta float64
	if elapsed >= 1 { // too noisy before a full second of samples
		speed = float64(done) / elapsed
	}
	if speed > 0 && done < r.total {
		eta = float64(r.total-done) / speed
	}
	line := render(r.label, done, r.total, chunksDone, chunksTotal,
		speed, time.Duration(eta*float64(time.Second)))
	switch {
	case r.tty && final:
		_, _ = fmt.Fprintf(r.out, "\r%s\n", line)
	case r.tty:
		_, _ = fmt.Fprintf(r.out, "\r%s", line)
	default:
		_, _ = fmt.Fprintln(r.out, line)
	}
}

func render(label string, done, total, chunksDone, chunksTotal uint64,
	speed float64, eta time.Duration,
) string {
	pct := 0.0
	if total > 0 {
		pct = 100 * float64(done) / float64(total)
	}
	etaStr := "--"
	if eta > 0 {
		etaStr = eta.Round(time.Second).String()
	}
	return fmt.Sprintf("%s  %s / %s  %.1f%%  %d/%d chunks  %s/s  ETA %s",
		label, FormatBytes(done), FormatBytes(total), pct,
		chunksDone, chunksTotal, FormatBytes(uint64(speed)), etaStr)
}
