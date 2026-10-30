// Package receiver implements the receiving side: a TCP server that
// stores incoming chunks and the on-disk state that makes resume work.
package receiver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/evgenymng/caravan/internal/protocol"
	"github.com/evgenymng/caravan/internal/transfer"
)

const (
	shutdownTimeout = 10 * time.Second
	// frameSlack covers the chunk header on top of the chunk data
	frameSlack = 64
)

// Config holds receiver settings.
type Config struct {
	Addr         string
	Dir          string
	MaxTransfers int
	MaxChunkSize uint32
	ReadTimeout  time.Duration
	Fsync        bool
	// AckDelay delays every ACK; used only to demonstrate sender timeouts.
	AckDelay time.Duration
	// OnChunk, if set, is called after every stored chunk.
	OnChunk func(meta transfer.Meta, received uint64)
}

// Server accepts sender connections and runs one session per connection.
type Server struct {
	cfg   Config
	store *Store
	ln    net.Listener
	sem   chan struct{}
	log   *slog.Logger
	wg    sync.WaitGroup
}

// session is the per-connection protocol state machine.
type session struct {
	*Server
	ctx  context.Context
	conn net.Conn
	log  *slog.Logger
}

// NewServer prepares the store and starts listening on cfg.Addr.
func NewServer(cfg Config, log *slog.Logger) (*Server, error) {
	store, err := NewStore(cfg.Dir, cfg.Fsync, cfg.MaxChunkSize)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	return &Server{
		cfg:   cfg,
		store: store,
		ln:    ln,
		sem:   make(chan struct{}, cfg.MaxTransfers),
		log:   log,
	}, nil
}

func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until ctx is cancelled, then waits for sessions to persist state.
func (s *Server) Serve(ctx context.Context) error {
	list, err := s.store.Recover()
	if err != nil {
		s.log.Warn("recover stored transfers", "error", err)
	}
	s.logStored(list)
	s.log.Info("listening", "addr", s.ln.Addr().String())

	sessions, cancelSessions := context.WithCancel(context.Background())
	defer cancelSessions()
	stop := context.AfterFunc(ctx, func() { _ = s.ln.Close() })
	defer stop()

	err = s.acceptLoop(sessions)
	s.log.Info("shutting down")
	cancelSessions()
	s.waitSessions()
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("accept: %w", err)
}

// acceptLoop runs a session per connection until Accept fails.
func (s *Server) acceptLoop(ctx context.Context) error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		s.wg.Go(func() { s.handle(ctx, conn) })
	}
}

func (s *Server) logStored(list []Summary) {
	for _, t := range list {
		s.log.Info(
			"stored transfer",
			"transfer_id", t.TransferID,
			"file", t.FileName,
			"status", t.Status,
			"received", t.ReceivedChunks,
			"chunks", t.Chunks,
		)
	}
}

// waitSessions gives active sessions shutdownTimeout to persist state.
func (s *Server) waitSessions() {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		s.log.Warn("shutdown timeout, sessions abandoned")
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	log := s.log.With("remote", conn.RemoteAddr().String())
	c := &session{Server: s, ctx: ctx, conn: conn, log: log}
	if err := c.run(); err != nil {
		log.Warn("session ended", "error", err)
	}
}

func (c *session) run() error {
	var hello protocol.Hello
	if err := c.expect(protocol.TypeHello, &hello); err != nil {
		return err
	}
	if hello.Version != protocol.Version {
		return c.fail(protocol.CodeBadVersion, fmt.Sprintf("unsupported version %d", hello.Version), nil)
	}
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	default:
		return c.fail(protocol.CodeBusy, "too many active transfers", nil)
	}
	ok := protocol.Hello{Version: protocol.Version}
	if err := c.write(protocol.TypeHelloOK, ok); err != nil {
		return err
	}

	var meta transfer.Meta
	if err := c.expect(protocol.TypeMeta, &meta); err != nil {
		return err
	}
	tr, err := c.store.Acquire(meta)
	if err != nil {
		return c.fail(codeFor(err), err.Error(), nil)
	}
	defer tr.Release(transfer.StatusInterrupted)
	c.log = c.log.With("transfer_id", tr.Meta().TransferID)
	c.log.Info(
		"transfer opened",
		"file", tr.Meta().FileName,
		"size", tr.Meta().FileSize,
		"chunks", tr.Meta().Chunks,
		"received", tr.ReceivedChunks(),
	)

	if err := c.write(protocol.TypeResumeState, protocol.ResumeState{
		TransferID:     tr.Meta().TransferID,
		Status:         tr.Status(),
		ChunkSize:      tr.Meta().ChunkSize,
		Chunks:         tr.Meta().Chunks,
		Received:       tr.Received(),
		ReceivedChunks: tr.ReceivedChunks(),
	}); err != nil {
		return err
	}
	return c.loop(tr)
}

func (c *session) loop(tr *Transfer) error {
	maxPayload := tr.Meta().ChunkSize + frameSlack
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		typ, payload, err := protocol.ReadFrame(c.conn, maxPayload)
		if err != nil {
			return fmt.Errorf("read frame: %w", err)
		}
		switch typ {
		case protocol.TypeChunk:
			if err := c.handleChunk(tr, payload); err != nil {
				return err
			}
		case protocol.TypeFinalize:
			return c.handleFinalize(tr)
		default:
			return fmt.Errorf("%w: %d", protocol.ErrUnexpectedType, typ)
		}
	}
}

// handleFinalize verifies and completes the transfer; the session ends either way.
func (c *session) handleFinalize(tr *Transfer) error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	_ = c.conn.SetReadDeadline(time.Time{})
	// sender is silent until COMPLETE: any read result means it left
	go func() { _, _ = c.conn.Read(make([]byte, 1)); cancel() }()

	path, err := tr.Finalize(ctx)
	if err != nil {
		return c.fail(codeFor(err), err.Error(), nil)
	}
	c.log.Info("transfer completed", "path", path, "checksum", tr.Meta().Checksum)
	return c.write(protocol.TypeComplete, protocol.Complete{
		TransferID: tr.Meta().TransferID,
		Checksum:   tr.Meta().Checksum,
	})
}

func (c *session) handleChunk(tr *Transfer, payload []byte) error {
	ch, err := protocol.DecodeChunk(payload)
	if err != nil {
		return c.fail(protocol.CodeBadChecksum, err.Error(), nil)
	}
	if !ch.Verify() {
		c.log.Warn("chunk checksum mismatch", "chunk", ch.Index)
		return c.fail(protocol.CodeBadChecksum, "checksum mismatch", &ch.Index)
	}
	dup, err := tr.WriteChunk(ch.Index, ch.Data)
	if err != nil {
		return c.fail(codeFor(err), err.Error(), &ch.Index)
	}
	c.log.Debug(
		"chunk stored",
		"chunk", ch.Index,
		"bytes", len(ch.Data),
		"sha256", fmt.Sprintf("%x", ch.Sum[:4]),
		"duplicate", dup,
	)
	if c.cfg.OnChunk != nil {
		c.cfg.OnChunk(tr.Meta(), tr.ReceivedChunks())
	}
	if c.cfg.AckDelay > 0 {
		time.Sleep(c.cfg.AckDelay)
	}
	return c.write(protocol.TypeAck, protocol.Ack{Index: ch.Index})
}

func (c *session) expect(t protocol.Type, v any) error {
	_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
	return protocol.Expect(c.conn, t, protocol.MaxControlPayload, v)
}

func (c *session) write(t protocol.Type, v any) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(protocol.WriteTimeout))
	return protocol.WriteJSON(c.conn, t, v)
}

// fail sends an ERROR frame and ends the session, unless the sender is expected to resend a bad chunk.
func (c *session) fail(code, msg string, index *uint64) error {
	e := &protocol.Error{Code: code, Message: msg, Index: index}
	if err := c.write(protocol.TypeError, e); err != nil {
		return err
	}
	if code == protocol.CodeBadChecksum {
		return nil
	}
	return e
}

func codeFor(err error) string {
	switch {
	case errors.Is(err, ErrBusy):
		return protocol.CodeBusy
	case errors.Is(err, transfer.ErrInvalidMeta):
		return protocol.CodeInvalidMeta
	case errors.Is(err, ErrBadChunk):
		return protocol.CodeBadChecksum
	case errors.Is(err, ErrOutOfRange):
		return protocol.CodeOutOfRange
	case errors.Is(err, ErrIncomplete):
		return protocol.CodeIncomplete
	case errors.Is(err, ErrChecksumMismatch):
		return protocol.CodeChecksumMismatch
	default:
		return protocol.CodeIOError
	}
}
