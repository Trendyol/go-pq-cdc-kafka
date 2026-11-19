package main

import (
	"context"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/mapper"
	"gopkg.in/yaml.v2"
)

/*
	The mapper of example/mapper, built from config.yml instead of code.

	cd example/mapper && docker compose up -d --wait && cd -

	docker compose -f example/mapper/docker-compose.yml exec -T postgres psql -U cdc_user -d cdc_db <<'SQL'
	CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, email text, password text);
	CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, status text NOT NULL);
	SQL

	cd example/mapper-config && go run .

	// from another terminal
	docker compose -f example/mapper/docker-compose.yml exec -T postgres psql -U cdc_user -d cdc_db <<'SQL'
	INSERT INTO users (name, email, password) VALUES ('ada', 'ada@example.com', 'secret');
	INSERT INTO orders (user_id, status) VALUES (1, 'created');
	DELETE FROM users WHERE id = 1;
	SQL

	docker compose -f example/mapper/docker-compose.yml exec redpanda rpk topic consume users.0 -n 1 --format '%k %h %v\n'
	// key 1, operation/table/source headers, no password, email masked, source_db added
	docker compose -f example/mapper/docker-compose.yml exec redpanda rpk topic consume users.deleted -n 1 --format '%k %h %v\n'
	// key 1, empty value (tombstone)
	docker compose -f example/mapper/docker-compose.yml exec redpanda rpk topic consume orders.0 -n 1 --format '%k %h %v\n'
	// key 1:1, value {"op", "before", "after", "source"} with customer_id
*/

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()

	cfg, err := readConfig("config.yml")
	if err != nil {
		slog.Error("read config", "error", err)
		os.Exit(1)
	}

	// The mappings come from config.yml. WithSteps adds a step written in code to one table;
	// any func(*mapper.Record) bool works. FromConfig reports every mistake in the mappings at once.
	handler, err := mapper.FromConfig(cfg.Kafka,
		mapper.WithSteps("users", mapper.Header("source", func(*mapper.Record) []byte { return []byte("example") })),
	)
	if err != nil {
		slog.Error("mappings", "error", err)
		os.Exit(1)
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func readConfig(path string) (config.Connector, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return config.Connector{}, err
	}
	var cfg config.Connector
	err = yaml.Unmarshal(b, &cfg)
	return cfg, err
}
