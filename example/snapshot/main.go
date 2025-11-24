package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	gokafka "github.com/segmentio/kafka-go"
)

/*
	This example demonstrates the snapshot feature in action.

	Snapshot Mode: "initial"
	- Takes a snapshot of existing data (users and books tables)
	- Then transitions to real-time CDC mode
	- Ensures zero data loss between snapshot and CDC phases

	The PostgreSQL database comes pre-populated with data via init.sql:
	- 1000 users
	- 500 books

	All this data will be captured via snapshot first, then any new changes
	will be captured via CDC.

	Key Features Demonstrated:
	1. Snapshot vs CDC Message Detection:
	   - Use msg.Type.IsSnapshot() to identify snapshot messages
	   - Use msg.Type.IsInsert(), IsUpdate(), IsDelete() for CDC operations

	2. Kafka Headers:
	   - "operation" header: SNAPSHOT, INSERT, UPDATE, or DELETE
	   - "source" header: "initial-snapshot" or "cdc"
	   - "table" header: fully qualified table name

	3. Message Body:
	   - "operation" field added to JSON payload for easy filtering
	   - All column data included in the message

	This allows downstream consumers to:
	- Distinguish between historical (snapshot) and real-time (CDC) data
	- Filter messages by operation type using headers
	- Process snapshot and CDC data differently if needed
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
					publication.OperationTruncate,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "users",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
					publication.Table{
						Name:            "books",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			// Snapshot configuration
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled:           true,
				Mode:              cdcconfig.SnapshotModeInitial, // Take snapshot only if no previous snapshot exists
				ChunkSize:         1000,                          // Process 1000 rows per chunk
				ClaimTimeout:      30 * time.Second,              // Reclaim timeout for stale chunks
				HeartbeatInterval: 5 * time.Second,               // Worker heartbeat interval
				// InstanceID is auto-generated if not specified
				// Tables field is optional - if not specified, all publication tables will be snapshotted
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
				"public.books": "books.0",
			},
			Brokers:                     []string{"localhost:19092"},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 200,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(msg *cdc.Message) []gokafka.Message {
	tableName := msg.TableNamespace + "." + msg.TableName

	switch {
	case msg.Type.IsSnapshot():
		return handleSnapshot(msg, tableName)
	case msg.Type.IsInsert():
		return handleInsert(msg, tableName)
	case msg.Type.IsUpdate():
		return handleUpdate(msg, tableName)
	case msg.Type.IsDelete():
		return handleDelete(msg, tableName)
	}

	return []gokafka.Message{}
}

func handleSnapshot(msg *cdc.Message, tableName string) []gokafka.Message {
	slog.Info("📸 snapshot data captured", "table", tableName, "type", "SNAPSHOT", "timestamp", msg.EventTime)
	return buildKafkaMessage(msg.NewData, tableName, "SNAPSHOT", "initial-snapshot")
}

func handleInsert(msg *cdc.Message, tableName string) []gokafka.Message {
	slog.Info("✨ insert captured", "table", tableName, "type", "INSERT", "timestamp", msg.EventTime)
	return buildKafkaMessage(msg.NewData, tableName, "INSERT", "cdc")
}

func handleUpdate(msg *cdc.Message, tableName string) []gokafka.Message {
	slog.Info("🔄 update captured", "table", tableName, "type", "UPDATE", "timestamp", msg.EventTime)
	return buildKafkaMessage(msg.NewData, tableName, "UPDATE", "cdc")
}

func handleDelete(msg *cdc.Message, tableName string) []gokafka.Message {
	slog.Info("🗑️  delete captured", "table", tableName, "type", "DELETE", "timestamp", msg.EventTime)
	return buildKafkaMessage(msg.OldData, tableName, "DELETE", "cdc")
}

func buildKafkaMessage(data map[string]any, tableName, operation, source string) []gokafka.Message {
	data["operation"] = operation
	payload, _ := json.Marshal(data)

	return []gokafka.Message{
		{
			Headers: []gokafka.Header{
				{Key: "operation", Value: []byte(operation)},
				{Key: "table", Value: []byte(tableName)},
				{Key: "source", Value: []byte(source)},
			},
			Key:   []byte(strconv.Itoa(int(data["id"].(int32)))),
			Value: payload,
		},
	}
}
