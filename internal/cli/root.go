// Package cli wires the caravan command-line interface: the root command,
// the send/receive/status subcommands and shared helpers.
package cli

import (
	"context"
	"log/slog"

	"github.com/spf13/cobra"
)

// NewRootCmd builds the root `caravan` command with all subcommands.
func NewRootCmd() *cobra.Command {
	var logLevel string

	root := &cobra.Command{
		Use:   "caravan",
		Short: "Resumable chunked file transfer over LAN",
		Long: "caravan transfers a file between two machines in a local " +
			"network in chunks with per-chunk integrity checks and " +
			"resume after a connection loss.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			var lvl slog.Level
			if err := lvl.UnmarshalText([]byte(logLevel)); err != nil {
				return err
			}
			log := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), &slog.HandlerOptions{Level: lvl}))
			slog.SetDefault(log)
			return nil
		},
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level: debug|info|warn|error")

	root.AddCommand(newSendCmd(), newReceiveCmd(), newStatusCmd())
	return root
}

// Execute runs the root command with ctx and returns its error.
func Execute(ctx context.Context) error {
	return NewRootCmd().ExecuteContext(ctx)
}
