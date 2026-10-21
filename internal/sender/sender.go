// Package sender implements the sending side: it hashes the file, talks
// the caravan protocol to a receiver and reconnects until the transfer
// is complete.
package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/evgenymng/caravan/internal/protocol"
	"github.com/evgenymng/caravan/internal/transfer"
)

const (
	writeTimeout   = 30 * time.Second
	maxBackoff     = 30 * time.Second
	maxChunkResend = 3
	controlPayload = 1 << 16
)

var (
	ErrSourceChanged = errors.New("source file changed since transfer started")
	errDropped       = errors.New("connection dropped on purpose")
)

// Options configures a Sender.
type Options struct {
	Path            string
	Addr            string
	ChunkSize       uint32
	MaxRetries      int // 0 means retry forever
	RetryDelay      time.Duration
	DialTimeout     time.Duration
	AckTimeout      time.Duration
	FinalizeTimeout time.Duration
	// DropAfterChunk closes the connection right after sending that chunk
	// without waiting for its ACK (once). -1 disables. Demo/test only.
	DropAfterChunk int64
	// OnProgress, if set, is called after every acknowledged chunk.
	OnProgress func(Stats)
}

// Stats is a snapshot of transfer counters.
type Stats struct {
	Meta       transfer.Meta
	Sent       uint64 // CHUNK frames written
	Acked      uint64 // ACKs received
	Skipped    uint64 // chunks the receiver already had on (re)connect
	Resent     uint64 // sends of a chunk this sender already saw ACKed
	BytesAcked uint64
	Reconnects int
}

// Sender drives one file transfer.
type Sender struct {
	opts Options
	log  *slog.Logger

	file    *os.File
	size    int64
	modTime time.Time
	meta    transfer.Meta
	dropped bool
	ackedBy *transfer.Bitmap // chunk indexes ACKed in this run

	sent, acked, skipped, resent, bytesAcked atomic.Uint64
	reconnects                               atomic.Int64
}

// New returns a Sender; nothing is opened until Run.
func New(opts Options, log *slog.Logger) *Sender {
	return &Sender{opts: opts, log: log}
}

// Stats returns the current counters.
func (s *Sender) Stats() Stats {
	return Stats{
		Meta:       s.meta,
		Sent:       s.sent.Load(),
		Acked:      s.acked.Load(),
		Skipped:    s.skipped.Load(),
		Resent:     s.resent.Load(),
		BytesAcked: s.bytesAcked.Load(),
		Reconnects: int(s.reconnects.Load()),
	}
}

// Run transfers the file, reconnecting on network errors, until the
// receiver confirms the whole file or a fatal error occurs.
func (s *Sender) Run(ctx context.Context) error {
	if err := s.open(ctx); err != nil {
		return err
	}
	defer s.close()

	delay := s.opts.RetryDelay
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			s.reconnects.Add(1)
			s.log.Info("reconnecting", "attempt", attempt, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			delay = min(2*delay, maxBackoff)
			if err := s.checkSource(); err != nil {
				return err
			}
		}
		err := s.attempt(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var perr *protocol.Error
		if errors.As(err, &perr) && perr.Fatal() {
			return err
		}
		s.log.Warn("transfer interrupted", "error", err)
		if s.opts.MaxRetries > 0 && attempt >= s.opts.MaxRetries {
			return fmt.Errorf("giving up after %d retries: %w", attempt, err)
		}
	}
}

func (s *Sender) open(ctx context.Context) error {
	f, err := os.Open(s.opts.Path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	s.file, s.size, s.modTime = f, st.Size(), st.ModTime()

	s.log.Info("hashing file", "path", s.opts.Path, "size", s.size)
	sum, err := transfer.HashReader(ctx, f)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("hash file: %w", err)
	}
	s.meta = transfer.NewMeta(filepath.Base(s.opts.Path),
		uint64(s.size), s.opts.ChunkSize, sum) //nolint:gosec // size ≥ 0
	s.log.Info("transfer prepared", "transfer_id", s.meta.TransferID,
		"chunks", s.meta.Chunks, "chunk_size", s.meta.ChunkSize,
		"sha256", sum)
	return nil
}

func (s *Sender) close() {
	if s.file != nil {
		_ = s.file.Close()
	}
}

// checkSource refuses to resume when the file on disk is no longer the
// one that was hashed: size or modification time changed.
func (s *Sender) checkSource() error {
	st, err := os.Stat(s.opts.Path)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSourceChanged, err)
	}
	if st.Size() != s.size || !st.ModTime().Equal(s.modTime) {
		return ErrSourceChanged
	}
	return nil
}

func (s *Sender) attempt(ctx context.Context) error {
	conn, err := net.DialTimeout("tcp", s.opts.Addr, s.opts.DialTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	return s.session(conn)
}

func (s *Sender) session(conn net.Conn) error {
	if err := s.write(conn, protocol.TypeHello,
		protocol.Hello{Version: protocol.Version}); err != nil {
		return err
	}
	var hello protocol.Hello
	err := s.expect(conn, protocol.TypeHelloOK, s.opts.AckTimeout, &hello)
	if err != nil {
		return err
	}
	if err := s.write(conn, protocol.TypeMeta, s.meta); err != nil {
		return err
	}
	var rs protocol.ResumeState
	err = s.expect(conn, protocol.TypeResumeState, s.opts.AckTimeout, &rs)
	if err != nil {
		return err
	}
	if rs.ChunkSize != s.meta.ChunkSize {
		s.log.Warn(
			"receiver already has this transfer with another chunk size, adopting it",
			"requested",
			s.meta.ChunkSize,
			"stored",
			rs.ChunkSize,
		)
		s.meta.ChunkSize, s.meta.Chunks = rs.ChunkSize, rs.Chunks
	}
	have := transfer.NewBitmap(s.meta.Chunks)
	for _, r := range rs.Received {
		for i := r[0]; i <= r[1]; i++ {
			have.Set(i)
		}
	}
	if s.ackedBy == nil || s.ackedBy.Len() != s.meta.Chunks {
		s.ackedBy = transfer.NewBitmap(s.meta.Chunks)
	}
	s.skipped.Store(have.Count())
	s.bytesAcked.Store(0)
	for _, r := range rs.Received {
		for i := r[0]; i <= r[1]; i++ {
			s.bytesAcked.Add(uint64(s.meta.ChunkLen(i)))
		}
	}
	s.log.Info("resume state", "status", rs.Status,
		"received", rs.ReceivedChunks, "chunks", s.meta.Chunks,
		"ranges", rs.Received)

	buf := make([]byte, s.meta.ChunkSize)
	for _, r := range have.Missing() {
		for i := r[0]; i <= r[1]; i++ {
			if err := s.sendChunk(conn, i, buf); err != nil {
				return err
			}
		}
	}

	if err := s.write(conn, protocol.TypeFinalize,
		protocol.Finalize{TransferID: s.meta.TransferID}); err != nil {
		return err
	}
	var done protocol.Complete
	err = s.expect(conn, protocol.TypeComplete, s.opts.FinalizeTimeout, &done)
	if err != nil {
		return err
	}
	if done.Checksum != s.meta.Checksum {
		return fmt.Errorf("receiver reported checksum %s, want %s",
			done.Checksum, s.meta.Checksum)
	}
	s.log.Info("transfer completed", "transfer_id", s.meta.TransferID,
		"sha256", done.Checksum)
	return nil
}

func (s *Sender) sendChunk(conn net.Conn, i uint64, buf []byte) error {
	data := buf[:s.meta.ChunkLen(i)]
	off := int64(i) * int64(s.meta.ChunkSize) //nolint:gosec // within file
	for try := 1; ; try++ {
		if _, err := s.file.ReadAt(data, off); err != nil {
			return fmt.Errorf("%w: read chunk %d: %w", ErrSourceChanged, i, err)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := protocol.WriteChunk(conn, i, data); err != nil {
			return err
		}
		s.sent.Add(1)
		if s.ackedBy.Has(i) {
			s.resent.Add(1)
			s.log.Error(
				"protocol bug: resending an acknowledged chunk",
				"chunk",
				i,
			)
		}
		s.log.Debug("chunk sent", "chunk", i, "bytes", len(data))
		if s.shouldDrop(i) {
			s.dropped = true
			s.log.Warn("dropping connection on purpose", "after_chunk", i)
			_ = conn.Close()
			return errDropped
		}

		var ack protocol.Ack
		err := s.expect(conn, protocol.TypeAck, s.opts.AckTimeout, &ack)
		var perr *protocol.Error
		if errors.As(err, &perr) && perr.Code == protocol.CodeBadChecksum &&
			try < maxChunkResend {
			s.log.Warn(
				"receiver rejected chunk, resending",
				"chunk",
				i,
				"try",
				try,
			)
			continue
		}
		if err != nil {
			return err
		}
		if ack.Index != i {
			return fmt.Errorf("ack for chunk %d, want %d", ack.Index, i)
		}
		s.acked.Add(1)
		s.ackedBy.Set(i)
		s.bytesAcked.Add(uint64(len(data)))
		s.log.Debug("chunk acked", "chunk", i)
		if s.opts.OnProgress != nil {
			s.opts.OnProgress(s.Stats())
		}
		return nil
	}
}

// shouldDrop implements the demo-only DropAfterChunk option (once).
func (s *Sender) shouldDrop(i uint64) bool {
	d := s.opts.DropAfterChunk
	if d < 0 || s.dropped || i != uint64(d) { //nolint:gosec // d ≥ 0
		return false
	}
	return true
}

func (s *Sender) write(conn net.Conn, t protocol.Type, v any) error {
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return protocol.WriteJSON(conn, t, v)
}

func (s *Sender) expect(
	conn net.Conn,
	t protocol.Type,
	timeout time.Duration,
	v any,
) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	return protocol.Expect(conn, t, controlPayload, v)
}
