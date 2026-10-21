package cli

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/evgenymng/caravan/internal/progress"
	"github.com/evgenymng/caravan/internal/receiver"
	"github.com/evgenymng/caravan/internal/transfer"
)

func newReceiveCmd() *cobra.Command {
	var (
		cfg          receiver.Config
		maxChunkSize string
		noFsync      bool
	)
	cmd := &cobra.Command{
		Use:   "receive",
		Short: "Accept incoming file transfers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			size, err := parseSize(maxChunkSize)
			if err != nil || size > 1<<30 {
				return fmt.Errorf("invalid --max-chunk-size %q", maxChunkSize)
			}
			cfg.MaxChunkSize = uint32(size) //nolint:gosec // ≤ 1 GiB
			cfg.Fsync = !noFsync

			var mu sync.Mutex
			bars := map[string]*progress.Reporter{}
			cfg.OnChunk = func(m transfer.Meta, received uint64) {
				mu.Lock()
				defer mu.Unlock()
				bar, ok := bars[m.TransferID]
				if !ok {
					bar = progress.New(cmd.OutOrStdout(), "recv "+m.FileName,
						m.FileSize, isTTY(os.Stdout))
					bars[m.TransferID] = bar
				}
				done := min(received*uint64(m.ChunkSize), m.FileSize)
				if received == m.Chunks {
					bar.Finish(done, m.Chunks)
					delete(bars, m.TransferID)
					return
				}
				bar.Update(done, received, m.Chunks)
			}

			srv, err := receiver.NewServer(cfg, slog.Default())
			if err != nil {
				return err
			}
			return srv.Serve(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.Addr, "addr", ":8080", "listen address")
	f.StringVar(&cfg.Dir, "dir", "./downloads", "directory for received files")
	f.IntVar(&cfg.MaxTransfers, "max-transfers", 4,
		"maximum concurrent transfers")
	f.StringVar(&maxChunkSize, "max-chunk-size", "64MB",
		"largest chunk size accepted from senders")
	f.DurationVar(&cfg.ReadTimeout, "read-timeout", 60*time.Second,
		"close idle connections after this long without a frame")
	f.BoolVar(&noFsync, "no-fsync", false,
		"do not fsync chunks and state before ACK (faster, less durable)")
	f.DurationVar(&cfg.AckDelay, "debug-ack-delay", 0,
		"delay every ACK (demonstrates sender ACK timeout)")
	_ = f.MarkHidden("debug-ack-delay")
	return cmd
}

func isTTY(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}
