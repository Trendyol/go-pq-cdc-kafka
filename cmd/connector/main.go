package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("connector", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := os.Getenv("CONFIG_YAML_PATH")
	if configPath == "" {
		configPath = "resources/config.yml"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := cdc.NewConnectorBuilder(configPath).Build(ctx)
	if err != nil {
		return fmt.Errorf("build connector: %w", err)
	}
	defer conn.Close()

	slog.Info("connector started")
	conn.Start(ctx)
	slog.Info("connector shutting down")
	return nil
}
