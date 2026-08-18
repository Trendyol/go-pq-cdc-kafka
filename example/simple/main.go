package main

import (
	"context"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
)

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	CREATE TABLE books (
	 id serial PRIMARY KEY,
	 author text NOT NULL,
	 created_on timestamptz
	);

    // after start cdc (because need to create publication and slot)

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;

	INSERT INTO books (author)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;
*/

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()

	connector, err := cdc.NewConnectorBuilder(configPath("example/simple/config.yml")).Build(ctx)
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
