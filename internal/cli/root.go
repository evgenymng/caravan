// Package cli wires the caravan command-line interface: the root command,
// the send/receive/status subcommands and shared helpers.
package cli

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

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
			lvl, err := parseLevel(logLevel)
			if err != nil {
				return err
			}
			log := slog.New(slog.NewTextHandler(
				cmd.ErrOrStderr(), &slog.HandlerOptions{Level: lvl}))
			slog.SetDefault(log)
			return nil
		},
	}
	root.PersistentFlags().StringVar(
		&logLevel, "log-level", "info", "log level: debug|info|warn|error")

	root.AddCommand(newSendCmd(), newReceiveCmd(), newStatusCmd())
	return root
}

// Execute runs the root command and returns its error.
func Execute() error {
	return NewRootCmd().Execute()
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

func notImplemented(cmd *cobra.Command, _ []string) error {
	fmt.Fprintf(os.Stderr, "%s: not implemented yet\n", cmd.Name())
	return nil
}
