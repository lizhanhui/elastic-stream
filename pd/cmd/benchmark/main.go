package main

import (
	"fmt"
	"os"

	"github.com/AutoMQ/pd/pkg/benchmark"
)

func main() {
	if err := benchmark.RootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}
