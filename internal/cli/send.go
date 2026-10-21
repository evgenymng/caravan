package cli

import (
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
			size, err := parseSize(chunkSize)
			if err != nil || size > 1<<30 {
				return fmt.Errorf("invalid --chunk-size %q", chunkSize)
			}
			opts.ChunkSize = uint32(size) //nolint:gosec // ≤ 1 GiB

			var bar *progress.Reporter
			opts.OnProgress = func(st sender.Stats) {
				if bar == nil {
					bar = progress.New(
						cmd.OutOrStdout(),
						"send "+st.Meta.FileName,
						st.Meta.FileSize,
						isTTY(os.Stdout),
					)
				}
				bar.Update(st.BytesAcked, st.Acked+st.Skipped, st.Meta.Chunks)
			}

			s := sender.New(opts, slog.Default())
			if err := s.Run(cmd.Context()); err != nil {
				return err
			}
			st := s.Stats()
			if bar == nil {
				bar = progress.New(cmd.OutOrStdout(), "send "+st.Meta.FileName,
					st.Meta.FileSize, isTTY(os.Stdout))
			}
			bar.Finish(st.Meta.FileSize, st.Meta.Chunks)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"completed: transfer %s, sha256 %s\n"+
					"chunks sent %d, acked %d, skipped on resume %d, "+
					"acked-and-resent %d, reconnects %d\n",
				st.Meta.TransferID, st.Meta.Checksum,
				st.Sent, st.Acked, st.Skipped, st.Resent, st.Reconnects)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.Path, "file", "", "file to send (required)")
	f.StringVar(&opts.Addr, "addr", "", "receiver address host:port (required)")
	f.StringVar(
		&chunkSize,
		"chunk-size",
		"1MB",
		"chunk size, e.g. 256KB, 1MB, 4MB",
	)
	f.IntVar(&opts.MaxRetries, "max-retries", 10,
		"reconnect attempts after a connection loss (0 = forever)")
	f.DurationVar(&opts.RetryDelay, "retry-delay", time.Second,
		"initial delay before reconnecting (doubles up to 30s)")
	f.DurationVar(&opts.DialTimeout, "dial-timeout", 5*time.Second,
		"TCP connect timeout")
	f.DurationVar(&opts.AckTimeout, "ack-timeout", 30*time.Second,
		"how long to wait for an ACK or handshake reply")
	f.DurationVar(&opts.FinalizeTimeout, "finalize-timeout", 5*time.Minute,
		"how long to wait for the receiver's whole-file verification")
	f.Int64Var(&opts.DropAfterChunk, "debug-drop-after-chunk", -1,
		"close the connection once right after sending this chunk")
	_ = f.MarkHidden("debug-drop-after-chunk")
	_ = cmd.MarkFlagRequired("file")
	_ = cmd.MarkFlagRequired("addr")
	return cmd
}
