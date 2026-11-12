package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/mapper"
	"github.com/Trendyol/go-pq-cdc/change"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 password text,
	 created_on timestamptz
	);

    // after start cdc (because need to create publication and slot)

	INSERT INTO users (name, password) VALUES ('Oyleli', 'secret');
	UPDATE users SET name = 'Oyleli Updated' WHERE name = 'Oyleli';
	DELETE FROM users WHERE name = 'Oyleli Updated';

	// read the topic: key = id, operation header, no password, tombstone for the delete
	docker compose exec redpanda rpk topic consume users.0 --format '%k %h %v\n'
*/

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      "127.0.0.1",
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "users",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.users": "users.0",
			},
			Brokers:                     []string{"localhost:19092"},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 200,
		},
	}

	m := mapper.New(
		mapper.Apply(
			change.OnlyOps(change.Insert, change.Update, change.Delete),
			change.Exclude("password"),
		),
		mapper.KeyFromColumns("id"),
		mapper.OperationHeader(),
		mapper.TombstoneOnDelete(),
	)

	connector, err := cdc.NewConnector(ctx, cfg, m)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}
