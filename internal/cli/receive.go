package cli

import "github.com/spf13/cobra"

func newReceiveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "receive",
		Short: "Accept incoming file transfers",
		RunE:  notImplemented,
	}
}
