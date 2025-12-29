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
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(`INSERT INTO users (name, email) VALUES ($1, $2)`,
			fmt.Sprintf("Test User %d", i),
			fmt.Sprintf("test%d@example.com", i))
		require.NoError(t, err)
	}

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

	// Read messages with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	// Read all insert messages
	messagesReceived := 0
	for i := 0; i < 5; i++ {
		message, err := reader.ReadMessage(msgCtx)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(message.Value, &data)
		require.NoError(t, err)

		assert.Equal(t, "INSERT", data["operation"])
		assert.Contains(t, data["name"], "Test User")
		assert.Contains(t, data["email"], "test")
		assert.NotNil(t, data["id"])
		messagesReceived++
	}

	assert.Equal(t, 5, messagesReceived, "Should receive 5 INSERT messages")
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
	userIDs := make([]int, 0, 5)
	for i := 1; i <= 5; i++ {
		var userID int
		err = db.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id`,
			fmt.Sprintf("Original User %d", i),
			fmt.Sprintf("original%d@example.com", i)).Scan(&userID)
		require.NoError(t, err)
		userIDs = append(userIDs, userID)
	}

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
	for i, userID := range userIDs {
		_, err = db.Exec(`UPDATE users SET name = $1, email = $2 WHERE id = $3`,
			fmt.Sprintf("Updated User %d", i+1),
			fmt.Sprintf("updated%d@example.com", i+1),
			userID)
		require.NoError(t, err)
	}

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

	// Read messages with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	// Read all update messages
	messagesReceived := 0
	receivedIDs := make(map[int]bool)
	for i := 0; i < 5; i++ {
		message, err := reader.ReadMessage(msgCtx)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(message.Value, &data)
		require.NoError(t, err)

		assert.Equal(t, "UPDATE", data["operation"])
		assert.Contains(t, data["name"], "Updated User")
		assert.Contains(t, data["email"], "updated")

		receivedID := int(data["id"].(float64))
		receivedIDs[receivedID] = true
		messagesReceived++
	}

	assert.Equal(t, 5, messagesReceived, "Should receive 5 UPDATE messages")
	assert.Equal(t, 5, len(receivedIDs), "Should receive updates for 5 different users")
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
	userIDs := make([]int, 0, 5)
	for i := 1; i <= 5; i++ {
		var userID int
		err = db.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id`,
			fmt.Sprintf("User To Delete %d", i),
			fmt.Sprintf("delete%d@example.com", i)).Scan(&userID)
		require.NoError(t, err)
		userIDs = append(userIDs, userID)
	}

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
	for _, userID := range userIDs {
		_, err = db.Exec(`DELETE FROM users WHERE id = $1`, userID)
		require.NoError(t, err)
	}

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

	// Read messages with timeout
	msgCtx, msgCancel := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel()

	// Read all delete messages
	messagesReceived := 0
	receivedIDs := make(map[int]bool)
	for i := 0; i < 5; i++ {
		message, err := reader.ReadMessage(msgCtx)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(message.Value, &data)
		require.NoError(t, err)

		assert.Equal(t, "DELETE", data["operation"])
		assert.Contains(t, data["name"], "User To Delete")
		assert.Contains(t, data["email"], "delete")

		receivedID := int(data["id"].(float64))
		receivedIDs[receivedID] = true
		messagesReceived++
	}

	assert.Equal(t, 5, messagesReceived, "Should receive 5 DELETE messages")
	assert.Equal(t, 5, len(receivedIDs), "Should receive deletes for 5 different users")
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
