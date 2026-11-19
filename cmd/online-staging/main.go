package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

// Build bindings are injected by the commit-pinned packaging driver. A plain
// development build is explicitly uncommitted, never a remote source binding.
var kafkaRevision = "uncommitted"
var coreRevision = "uncommitted"
var sourceState = "uncommitted"

func main() {
	if err := execute(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "online-staging: "+err.Error())
		os.Exit(1)
	}
}

func execute(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("online-staging", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	version := flags.Bool("version", false, "print source bindings without loading configuration or connecting")
	configFile := flags.String("config", "", "mounted runtime YAML configuration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("invalid arguments")
	}
	if *version {
		return json.NewEncoder(output).Encode(map[string]string{
			"kafkaRevision": kafkaRevision, "coreRevision": coreRevision, "goVersion": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH, "sourceState": sourceState,
		})
	}
	if *configFile == "" {
		return errors.New("configuration required")
	}
	cfg, err := loadRuntimeConfig(*configFile)
	if err != nil {
		return errConfiguration
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return runCapture(ctx, cfg, newCapture, output)
}
