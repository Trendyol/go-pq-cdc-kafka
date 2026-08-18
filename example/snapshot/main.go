package main

import (
	"context"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
)

/*
	This example demonstrates the snapshot feature via YAML config.

	Snapshot Mode: "initial"
	- Takes a snapshot of existing data (users and books tables)
	- Then transitions to real-time CDC mode
	- Ensures zero data loss between snapshot and CDC phases

	The PostgreSQL database comes pre-populated with data via init.sql:
	- 1000 users
	- 500 books

	All this data will be captured via snapshot first, then any new changes
	will be captured via CDC.

	Kafka headers (configured in config.yml mapper.headers):
	  - operation: SNAPSHOT, INSERT, UPDATE, or DELETE
	  - table: fully qualified table name
	  - source: initial-snapshot or cdc
*/

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()

	connector, err := cdc.NewConnectorBuilder(configPath("example/snapshot/config.yml")).Build(ctx)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func configPath(fallback string) string {
	if _, err := os.Stat("./config.yml"); err == nil {
		return "./config.yml"
	}
	return fallback
}
