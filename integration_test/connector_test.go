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
	assert.Len(t, receivedIDs, 5, "Should receive updates for 5 different users")
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
	assert.Len(t, receivedIDs, 5, "Should receive deletes for 5 different users")
}

func TestConnector_AckMechanism(t *testing.T) {
	ctx := context.Background()

	// Setup database schema
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			price DECIMAL(10,2),
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Setup connector with small batch size to trigger flush
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
				Name:              "cdc_publication_test_ack",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            "products",
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot_test_ack",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				"public.products": "products.test.ack",
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           5, // Small batch size to trigger flush quickly
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)

	// Start connector in background
	go connector.Start(ctx)

	// Wait for connector to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)

	// Insert first batch of data (5 products to fill the batch)
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(`INSERT INTO products (name, price) VALUES ($1, $2)`,
			fmt.Sprintf("Product Batch 1 - %d", i),
			float64(i)*10.50)
		require.NoError(t, err)
	}

	// Setup Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "products.test.ack",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to beginning
	reader.SetOffset(kafka.FirstOffset)

	// Read first batch messages (should be 5)
	msgCtx1, msgCancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel1()

	firstBatchMessages := 0
	for i := 0; i < 5; i++ {
		message, err := reader.ReadMessage(msgCtx1)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(message.Value, &data)
		require.NoError(t, err)

		assert.Equal(t, "INSERT", data["operation"])
		assert.Contains(t, data["name"], "Product Batch 1")
		firstBatchMessages++
	}
	assert.Equal(t, 5, firstBatchMessages, "Should receive 5 messages from first batch")

	// Wait for batch to be acked (batch ticker + processing time)
	time.Sleep(time.Second * 2)

	// Close connector
	connector.Close()

	// Wait a bit for clean shutdown
	time.Sleep(time.Second * 1)

	// Insert second batch while connector is down (these should be captured when we restart)
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(`INSERT INTO products (name, price) VALUES ($1, $2)`,
			fmt.Sprintf("Product Batch 2 - %d", i),
			float64(i)*20.50)
		require.NoError(t, err)
	}

	// Restart connector with same configuration
	connector2, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector2.Close()

	go connector2.Start(ctx)

	// Wait for connector to be ready
	readyCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()
	err = connector2.WaitUntilReady(readyCtx2)
	require.NoError(t, err)

	// Create new reader to read all messages from beginning
	reader2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     "products.test.ack",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader2.Close()

	reader2.SetOffset(kafka.FirstOffset)

	// Read all messages (should have first batch + second batch = 10 total)
	msgCtx2, msgCancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer msgCancel2()

	allMessages := make([]map[string]interface{}, 0)
	for i := 0; i < 10; i++ {
		message, err := reader2.ReadMessage(msgCtx2)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(message.Value, &data)
		require.NoError(t, err)
		allMessages = append(allMessages, data)
	}

	// Verify we have exactly 10 messages
	assert.Len(t, allMessages, 10, "Should have exactly 10 messages in Kafka")

	// Verify first 5 are from batch 1 (these were acked, so stream moved forward)
	batch1Count := 0
	batch2Count := 0
	for _, msg := range allMessages {
		if name, ok := msg["name"].(string); ok {
			if contains(name, "Product Batch 1") {
				batch1Count++
			}
			if contains(name, "Product Batch 2") {
				batch2Count++
			}
		}
	}

	// Both batches should be in Kafka
	assert.Equal(t, 5, batch1Count, "Should have 5 messages from batch 1")
	assert.Equal(t, 5, batch2Count, "Should have 5 messages from batch 2")

	// The key test: After restart, connector should continue from where it acked
	// This means first batch was acknowledged and stream moved forward
	// Second batch (inserted while connector was down) should be captured
	// If ack didn't work, we would see duplicates or missing messages

	// Insert one more message after connector restart to verify stream is working
	var productID int
	err = db.QueryRow(`INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id`,
		"Product After Restart",
		99.99).Scan(&productID)
	require.NoError(t, err)

	// Wait for the new message
	time.Sleep(time.Second * 2)

	// Try to read the new message
	msgCtx3, msgCancel3 := context.WithTimeout(ctx, 5*time.Second)
	defer msgCancel3()

	// Position reader at offset 10 (after the 10 messages we already read)
	reader2.SetOffset(10)

	message, err := reader2.ReadMessage(msgCtx3)
	require.NoError(t, err)

	var data map[string]interface{}
	err = json.Unmarshal(message.Value, &data)
	require.NoError(t, err)

	assert.Equal(t, "INSERT", data["operation"])
	assert.Equal(t, "Product After Restart", data["name"])

	// This proves the ack mechanism works:
	// 1. First batch was inserted and acked
	// 2. Connector stopped
	// 3. Second batch inserted while down
	// 4. Connector restarted and continued from acked position
	// 5. Second batch was captured (not re-captured first batch)
	// 6. New messages after restart are also captured correctly
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
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
