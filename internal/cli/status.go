package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/evgenymng/caravan/internal/receiver"
)

func newStatusCmd() *cobra.Command {
	var dir string
	cmd := &cobra.Command{
		Use:   "status [transferId]",
		Short: "Show state of stored transfers",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := receiver.NewStore(dir, false)
			if err != nil {
				return err
			}
			list, err := store.Scan()
			if err != nil {
				return err
			}
			found := false
			for _, t := range list {
				if len(args) == 1 && t.TransferID != args[0] {
					continue
				}
				found = true
				done := t.ReceivedChunks * uint64(t.ChunkSize)
				if done > t.FileSize {
					done = t.FileSize
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"transfer: %s\nfile: %s\nstatus: %s\n"+
						"received: %d / %d chunks\nprogress: %.1f %%\n"+
						"received: %s / %s\nupdated: %s\n\n",
					t.TransferID, t.FileName, t.Status,
					t.ReceivedChunks, t.Chunks,
					100*float64(t.ReceivedChunks)/float64(max(t.Chunks, 1)),
					formatBytes(done), formatBytes(t.FileSize),
					t.UpdatedAt.Format("2006-01-02 15:04:05"))
			}
			if !found {
				if len(args) == 1 {
					return fmt.Errorf(
						"transfer %s not found in %s",
						args[0],
						dir,
					)
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "no stored transfers")
			}
			return nil
		},
	}
	cmd.Flags().
		StringVar(&dir, "dir", "./downloads", "receiver downloads directory")
	return cmd
}
