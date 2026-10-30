package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/evgenymng/caravan/internal/progress"
	"github.com/evgenymng/caravan/internal/receiver"
)

func newStatusCmd() *cobra.Command {
	var dir string
	cmd := &cobra.Command{
		Use:   "status [transferId]",
		Short: "Show state of stored transfers",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			list, err := receiver.Scan(dir)
			if err != nil {
				return err
			}
			found := false
			for _, t := range list {
				if len(args) == 1 && t.TransferID != args[0] {
					continue
				}
				found = true
				printSummary(cmd.OutOrStdout(), t)
			}
			if found {
				return nil
			}
			if len(args) == 1 {
				return fmt.Errorf("transfer %s not found in %s", args[0], dir)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "no stored transfers")
			return nil
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "./downloads", "receiver downloads directory")
	return cmd
}

func printSummary(w io.Writer, t receiver.Summary) {
	done := t.ChunksLen(t.ReceivedChunks)
	pct := 100 * float64(t.ReceivedChunks) / float64(max(t.Chunks, 1))
	_, _ = fmt.Fprintf(w, "transfer: %s\nfile: %s\nstatus: %s\n", t.TransferID, t.FileName, t.Status)
	_, _ = fmt.Fprintf(w, "received: %d / %d chunks\nprogress: %.1f %%\n", t.ReceivedChunks, t.Chunks, pct)
	_, _ = fmt.Fprintf(w, "received: %s / %s\n", progress.FormatBytes(done), progress.FormatBytes(t.FileSize))
	_, _ = fmt.Fprintf(w, "updated: %s\n\n", t.UpdatedAt.Format(time.DateTime))
}
