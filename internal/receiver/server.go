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
	writeTimeout    = 30 * time.Second
	shutdownTimeout = 10 * time.Second
	// frameSlack covers the chunk header on top of the chunk data.
	frameSlack = 64
	// controlPayload bounds JSON frames; they are read before any
	// per-transfer limit applies, so this caps memory per connection.
	controlPayload = 64 << 10
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
	conn net.Conn
	log  *slog.Logger
}

// NewServer prepares the store and starts listening on cfg.Addr.
func NewServer(cfg Config, log *slog.Logger) (*Server, error) {
	store, err := NewStore(cfg.Dir, cfg.Fsync)
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

// Addr returns the bound listener address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Store exposes the state store (for `status`-style queries).
func (s *Server) Store() *Store { return s.store }

// Serve accepts connections until ctx is cancelled, then closes the
// listener, aborts active sessions and waits for them to persist state.
func (s *Server) Serve(ctx context.Context) error {
	if err := s.store.Recover(); err != nil {
		s.log.Warn("recover stored transfers", "error", err)
	}
	if list, err := s.store.Scan(); err == nil {
		for _, t := range list {
			s.log.Info("stored transfer", "transfer_id", t.TransferID,
				"file", t.FileName, "status", t.Status,
				"received", t.ReceivedChunks, "chunks", t.Chunks)
		}
	}
	s.log.Info("listening", "addr", s.ln.Addr().String())

	sessions, cancelSessions := context.WithCancel(context.Background())
	defer cancelSessions()
	stop := context.AfterFunc(ctx, func() { _ = s.ln.Close() })
	defer stop()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				return fmt.Errorf("accept: %w", err)
			}
			break
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(sessions, conn)
		}()
	}

	s.log.Info("shutting down")
	cancelSessions()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		s.log.Warn("shutdown timeout, sessions abandoned")
	}
	return nil
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	log := s.log.With("remote", conn.RemoteAddr().String())
	c := &session{Server: s, conn: conn, log: log}
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
		return c.fail(protocol.CodeBadVersion,
			fmt.Sprintf("unsupported version %d", hello.Version), nil)
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

	var meta protocol.Meta
	if err := c.expect(protocol.TypeMeta, &meta); err != nil {
		return err
	}
	if err := meta.Validate(c.cfg.MaxChunkSize); err != nil {
		return c.fail(protocol.CodeInvalidMeta, err.Error(), nil)
	}
	tr, err := c.store.Acquire(meta, func() { _ = c.conn.Close() })
	if err != nil {
		return c.fail(codeFor(err), err.Error(), nil)
	}
	defer tr.Release(transfer.StatusInterrupted)
	c.log = c.log.With("transfer_id", tr.Meta().TransferID)
	c.log.Info("transfer opened", "file", tr.Meta().FileName,
		"size", tr.Meta().FileSize, "chunks", tr.Meta().Chunks,
		"received", tr.ReceivedChunks())

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
	maxPayload := c.cfg.MaxChunkSize + frameSlack
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
			path, err := tr.Finalize()
			switch {
			case errors.Is(err, ErrIncomplete):
				err = c.fail(protocol.CodeIncomplete, err.Error(), nil)
				if err != nil {
					return err
				}
				continue
			case err != nil:
				return c.fail(codeFor(err), err.Error(), nil)
			}
			c.log.Info("transfer completed", "path", path,
				"checksum", tr.Meta().Checksum)
			return c.write(protocol.TypeComplete, protocol.Complete{
				TransferID: tr.Meta().TransferID, Checksum: tr.Meta().Checksum,
			})
		default:
			return fmt.Errorf("%w: %d", protocol.ErrUnexpectedType, typ)
		}
	}
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
	switch {
	case errors.Is(err, ErrBadChunk):
		return c.fail(protocol.CodeBadChecksum, err.Error(), &ch.Index)
	case err != nil:
		return c.fail(codeFor(err), err.Error(), &ch.Index)
	}
	c.log.Debug("chunk stored", "chunk", ch.Index, "bytes", len(ch.Data),
		"sha256", fmt.Sprintf("%x", ch.Sum[:4]), "duplicate", dup)
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
	return protocol.Expect(c.conn, t, controlPayload, v)
}

func (c *session) write(t protocol.Type, v any) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return protocol.WriteJSON(c.conn, t, v)
}

// fail sends an ERROR frame. For non-fatal codes it returns nil so the
// session can continue; for fatal ones it returns the error to end it.
func (c *session) fail(code, msg string, index *uint64) error {
	if err := c.write(protocol.TypeError, protocol.Error{
		Code: code, Message: msg, Index: index,
	}); err != nil {
		return err
	}
	e := &protocol.Error{Code: code, Message: msg, Index: index}
	if e.Fatal() || code == protocol.CodeBusy || code == protocol.CodeIOError {
		return e
	}
	return nil
}

func codeFor(err error) string {
	switch {
	case errors.Is(err, ErrBusy):
		return protocol.CodeBusy
	case errors.Is(err, ErrMetaMismatch):
		return protocol.CodeMetaMismatch
	case errors.Is(err, transfer.ErrInvalidMeta):
		return protocol.CodeInvalidMeta
	case errors.Is(err, ErrOutOfRange):
		return protocol.CodeOutOfRange
	case errors.Is(err, ErrChecksumMismatch):
		return protocol.CodeChecksumMismatch
	default:
		return protocol.CodeIOError
	}
}
