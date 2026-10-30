package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/evgenymng/caravan/internal/progress"
	"github.com/evgenymng/caravan/internal/sender"
)

func newSendCmd() *cobra.Command {
	var (
		opts      sender.Options
		chunkSize string
	)
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Send a file to a receiver",
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			if opts.ChunkSize, err = parseChunkSize("chunk-size", chunkSize); err != nil {
				return err
			}
			err = errors.Join(
				positive("retry-delay", opts.RetryDelay),
				positive("ack-timeout", opts.AckTimeout),
				positive("finalize-timeout", opts.FinalizeTimeout),
			)
			if err != nil {
				return err
			}

			var bar *progress.Reporter
			barFor := func(st sender.Stats) *progress.Reporter {
				if bar == nil {
					m := st.Meta
					bar = progress.New(
						cmd.OutOrStdout(),
						"send "+m.FileName,
						m.FileSize,
						st.BytesAcked,
						isTTY(os.Stdout),
					)
				}
				return bar
			}
			opts.OnProgress = func(st sender.Stats) {
				barFor(st).Update(st.BytesAcked, st.Done, st.Meta.Chunks)
			}

			s := sender.New(opts, slog.Default())
			if err := s.Run(cmd.Context()); err != nil {
				return err
			}
			st := s.Stats()
			barFor(st).Finish(st.Meta.FileSize, st.Meta.Chunks)
			out, m := cmd.OutOrStdout(), st.Meta
			_, _ = fmt.Fprintf(out, "completed: transfer %s, sha256 %s\n", m.TransferID, m.Checksum)
			_, _ = fmt.Fprintf(
				out,
				"chunks sent %d, acked %d, skipped on resume %d, acked-and-resent %d, reconnects %d\n",
				st.Sent,
				st.Acked,
				st.Skipped,
				st.Resent,
				st.Reconnects,
			)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.Path, "file", "", "file to send (required)")
	f.StringVar(&opts.Addr, "addr", "", "receiver address host:port (required)")
	f.StringVar(&chunkSize, "chunk-size", "1MB", "chunk size, 4KB..64MB, e.g. 256KB, 1MB, 4MB")
	f.IntVar(&opts.MaxRetries, "max-retries", 10, "reconnect attempts after a connection loss (0 = forever)")
	f.DurationVar(&opts.RetryDelay, "retry-delay", time.Second, "first reconnect delay, doubles up to 30s")
	f.DurationVar(&opts.DialTimeout, "dial-timeout", 5*time.Second, "TCP connect timeout")
	f.DurationVar(&opts.AckTimeout, "ack-timeout", 30*time.Second, "ACK and handshake reply timeout")
	f.DurationVar(&opts.FinalizeTimeout, "finalize-timeout", 5*time.Minute, "whole-file check timeout")
	f.Int64Var(&opts.DropAfterChunk, "debug-drop-after-chunk", -1, "drop the link once after this chunk")
	_ = f.MarkHidden("debug-drop-after-chunk")
	_ = cmd.MarkFlagRequired("file")
	_ = cmd.MarkFlagRequired("addr")
	return cmd
}
