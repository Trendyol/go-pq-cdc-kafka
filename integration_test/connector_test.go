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

func TestConnector_InsertOperation(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Setup connector
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication_test",
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
				Name:                        "cdc_slot_test",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.users": "users.test",
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector in background
	go connector.Start(ctx)

	// Wait for connector to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (name, email) VALUES ($1, $2)`, "Test User", "test@example.com")
	require.NoError(t, err)

	// Setup Kafka reader to read from beginning
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "users.test",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to beginning
	reader.SetOffset(kafka.FirstOffset)

	// Read message with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	message, err := reader.ReadMessage(msgCtx)
	require.NoError(t, err)

	// Verify message
	var data map[string]interface{}
	err = json.Unmarshal(message.Value, &data)
	require.NoError(t, err)

	assert.Equal(t, "INSERT", data["operation"])
	assert.Equal(t, "Test User", data["name"])
	assert.Equal(t, "test@example.com", data["email"])
	assert.NotNil(t, data["id"])
}

func TestConnector_UpdateOperation(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	var userID int
	err = db.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id`, "Original User", "original@example.com").Scan(&userID)
	require.NoError(t, err)

	// Setup connector
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication_test_update",
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
				Name:                        "cdc_slot_test_update",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.users": "users.test.update",
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector in background
	go connector.Start(ctx)

	// Wait for connector to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Update data
	_, err = db.Exec(`UPDATE users SET name = $1, email = $2 WHERE id = $3`, "Updated User", "updated@example.com", userID)
	require.NoError(t, err)

	// Setup Kafka reader to read from beginning
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "users.test.update",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to beginning
	reader.SetOffset(kafka.FirstOffset)

	// Read message with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	message, err := reader.ReadMessage(msgCtx)
	require.NoError(t, err)

	// Verify message
	var data map[string]interface{}
	err = json.Unmarshal(message.Value, &data)
	require.NoError(t, err)

	assert.Equal(t, "UPDATE", data["operation"])
	assert.Equal(t, "Updated User", data["name"])
	assert.Equal(t, "updated@example.com", data["email"])
	assert.Equal(t, userID, int(data["id"].(float64)))
}

func TestConnector_DeleteOperation(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	var userID int
	err = db.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id`, "User To Delete", "delete@example.com").Scan(&userID)
	require.NoError(t, err)

	// Setup connector
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication_test_delete",
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
				Name:                        "cdc_slot_test_delete",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.users": "users.test.delete",
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()

	// Start connector in background
	go connector.Start(ctx)

	// Wait for connector to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Delete data
	_, err = db.Exec(`DELETE FROM users WHERE id = $1`, userID)
	require.NoError(t, err)

	// Setup Kafka reader to read from beginning
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "users.test.delete",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to beginning
	reader.SetOffset(kafka.FirstOffset)

	// Read message with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	message, err := reader.ReadMessage(msgCtx)
	require.NoError(t, err)

	// Verify message
	var data map[string]interface{}
	err = json.Unmarshal(message.Value, &data)
	require.NoError(t, err)

	assert.Equal(t, "DELETE", data["operation"])
	assert.Equal(t, "User To Delete", data["name"])
	assert.Equal(t, "delete@example.com", data["email"])
	assert.Equal(t, userID, int(data["id"].(float64)))
}

func handler(msg *cdc.Message) []kafka.Message {
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
