package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_SnapshotMode(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS books (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert initial data before starting CDC
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`INSERT INTO books (title, author) VALUES ($1, $2)`, fmt.Sprintf("Book %d", i), fmt.Sprintf("Author %d", i))
		require.NoError(t, err)
	}

	// Setup connector with snapshot mode
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled: true,
				Mode:    cdcconfig.SnapshotModeInitial,
			},
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication_test_snapshot",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "books",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot_test_snapshot",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.books": "books.test.snapshot",
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, snapshotHandler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector in background (snapshot mode runs synchronously in some cases)
	go connector.Start(ctx)

	// Wait a bit for snapshot to complete
	time.Sleep(5 * time.Second)

	// Setup Kafka reader to read from beginning
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "books.test.snapshot",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to beginning
	reader.SetOffset(kafka.FirstOffset)

	// Read all snapshot messages
	messages := make([]kafka.Message, 0)
	msgCtx, msgCancel := context.WithTimeout(ctx, 30*time.Second)
	defer msgCancel()

	for i := 0; i < 10; i++ {
		message, err := reader.ReadMessage(msgCtx)
		if err != nil {
			break
		}
		messages = append(messages, message)
	}

	// Verify we received snapshot messages
	assert.GreaterOrEqual(t, len(messages), 10, "Should receive at least 10 snapshot messages")

	// Verify first message
	var data map[string]interface{}
	err = json.Unmarshal(messages[0].Value, &data)
	require.NoError(t, err)

	assert.Equal(t, "SNAPSHOT", data["operation"])
	assert.Contains(t, data, "title")
	assert.Contains(t, data, "author")
	assert.Contains(t, data, "id")
}

func snapshotHandler(msg *cdc.Message) []kafka.Message {
	if msg.Type.IsSnapshot() {
		msg.NewData["operation"] = "SNAPSHOT"
		newData, _ := json.Marshal(msg.NewData)

		key := ""
		if id, ok := msg.NewData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []kafka.Message{
			{
				Key:   []byte(key),
				Value: newData,
			},
		}
	}

	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		key := ""
		if id, ok := msg.NewData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []kafka.Message{
			{
				Key:   []byte(key),
				Value: newData,
			},
		}
	}

	if msg.Type.IsDelete() {
		msg.OldData["operation"] = msg.Type
		oldData, _ := json.Marshal(msg.OldData)

		key := ""
		if id, ok := msg.OldData["id"]; ok {
			switch v := id.(type) {
			case int32:
				key = strconv.Itoa(int(v))
			case int64:
				key = strconv.FormatInt(v, 10)
			case float64:
				key = strconv.FormatFloat(v, 'f', 0, 64)
			}
		}

		return []kafka.Message{
			{
				Key:   []byte(key),
				Value: oldData,
			},
		}
	}

	return []kafka.Message{}
}
