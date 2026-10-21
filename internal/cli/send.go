package cli

import "github.com/spf13/cobra"

func newSendCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "send",
		Short: "Send a file to a receiver",
		RunE:  notImplemented,
	}
}
