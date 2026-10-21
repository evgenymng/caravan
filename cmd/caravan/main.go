package main

import (
	"fmt"
	"os"

	"github.com/evgenymng/caravan/internal/cli"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	return cli.Execute()
}
