// Package tests holds end-to-end scenarios: a real receiver, a real sender
// and, where needed, a TCP proxy that breaks or corrupts the stream.
package tests

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/evgenymng/caravan/internal/receiver"
	"github.com/evgenymng/caravan/internal/sender"
	"github.com/stretchr/testify/require"
)

const (
	chunk64K  = 64 << 10
	typeChunk = 5
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type testReceiver struct {
	*receiver.Server
	cancel context.CancelFunc
	wait   func() error
}

// proxy forwards TCP to target and can, once, kill the connection right
// after forwarding the n-th CHUNK frame or corrupt the n-th chunk's data.
type proxy struct {
	ln     net.Listener
	target string

	mu           sync.Mutex
	chunks       int
	killAfter    int // 0 = never
	killEvery    int // 0 = never; kill after every n-th chunk
	corruptChunk int // 0 = never
	killed       chan struct{}
}

func tempFile(t *testing.T, name string, size int) (string, string) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	sum := sha256.Sum256(data)
	return path, hex.EncodeToString(sum[:])
}

func fileSum(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func startReceiver(t *testing.T, cfg receiver.Config) *testReceiver {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.MaxTransfers == 0 {
		cfg.MaxTransfers = 2
	}
	if cfg.MaxChunkSize == 0 {
		cfg.MaxChunkSize = 4 << 20
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	srv, err := receiver.NewServer(cfg, discard)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	wait := sync.OnceValue(func() error { return <-done })
	t.Cleanup(func() { cancel(); _ = wait() })
	return &testReceiver{Server: srv, cancel: cancel, wait: wait}
}

func senderOpts(path, addr string, chunkSize uint32) sender.Options {
	return sender.Options{
		Path:            path,
		Addr:            addr,
		ChunkSize:       chunkSize,
		MaxRetries:      20,
		RetryDelay:      50 * time.Millisecond,
		DialTimeout:     time.Second,
		AckTimeout:      5 * time.Second,
		FinalizeTimeout: 30 * time.Second,
		DropAfterChunk:  -1,
	}
}

func startProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &proxy{ln: ln, target: target, killed: make(chan struct{})}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(client)
	}
}

func (p *proxy) handle(client net.Conn) {
	defer func() { _ = client.Close() }()
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	go func() { _, _ = io.Copy(client, server) }()

	var hdr [5]byte
	for {
		if _, err := io.ReadFull(client, hdr[:]); err != nil {
			return
		}
		payload := make([]byte, binary.BigEndian.Uint32(hdr[:])-1)
		if _, err := io.ReadFull(client, payload); err != nil {
			return
		}
		kill := false
		if hdr[4] == typeChunk {
			p.mu.Lock()
			p.chunks++
			if p.chunks == p.corruptChunk {
				payload[len(payload)-1] ^= 0xff
			}
			if p.chunks == p.killAfter {
				kill = true
				p.killAfter = 0
			}
			if p.killEvery > 0 && p.chunks%p.killEvery == 0 {
				kill = true
			}
			p.mu.Unlock()
		}
		if _, err := server.Write(append(hdr[:], payload...)); err != nil {
			return
		}
		if kill {
			select {
			case <-p.killed:
			default:
				close(p.killed)
			}
			return
		}
	}
}
