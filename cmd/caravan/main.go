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
	err := run()
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "interrupted")
		os.Exit(130) // like a shell, so `caravan send ... && rm f` keeps f
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return cli.Execute(ctx)
}
