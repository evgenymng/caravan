package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/evgenymng/caravan/internal/cli"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()

	err := cli.Execute(ctx)
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "interrupted")
		return nil
	}
	return err
}
