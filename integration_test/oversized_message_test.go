package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
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

const (
	oversizedPayloadSize = 20 * 1024 // comfortably above 10kb pre-filter limit
	oversizedMaxBytes    = "10kb"
)

func TestConnector_SkipOversizedMessages_MixedBatchDeliversValidMessages(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "oversized_events_mixed"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "oversized.test.mixed"
	cfg := oversizedConnectorConfig(
		"cdc_slot_oversized_mixed",
		"cdc_publication_oversized_mixed",
		tableName,
		topic,
		true,
		oversizedMaxBytes,
		20,
	)

	connector, err := cdc.NewConnector(ctx, cfg, oversizedPayloadHandler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(t, ctx, connector)

	// 10 valid + 1 oversized + 4 valid = 15 messages; batch size 15 triggers flush.
	for i := 1; i <= 10; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("valid-%d", i), smallPayload(i))
	}
	insertOversizedEvent(t, db, tableName, "oversized-middle", largePayload())
	for i := 11; i <= 14; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("valid-%d", i), smallPayload(i))
	}

	messages := readTopicMessages(t, topic, 14, 15*time.Second)
	names := messageNames(messages)

	assert.Len(t, names, 14, "oversized event must not block valid messages in the batch")
	assert.NotContains(t, names, "oversized-middle")
	for i := 1; i <= 14; i++ {
		assert.Contains(t, names, fmt.Sprintf("valid-%d", i))
	}
}

func TestConnector_SkipOversizedMessages_SamePartitionKeyDeliversValidMessages(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "oversized_events_partition"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "oversized.test.partition"
	cfg := oversizedConnectorConfig(
		"cdc_slot_oversized_partition",
		"cdc_publication_oversized_partition",
		tableName,
		topic,
		true,
		oversizedMaxBytes,
		13,
	)

	connector, err := cdc.NewConnector(ctx, cfg, samePartitionOversizedHandler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(t, ctx, connector)

	// Same Kafka key for all rows → same partition batch.
	for i := 1; i <= 8; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("same-partition-valid-%d", i), smallPayload(i))
	}
	insertOversizedEvent(t, db, tableName, "same-partition-oversized", largePayload())
	for i := 9; i <= 12; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("same-partition-valid-%d", i), smallPayload(i))
	}

	messages := readTopicMessages(t, topic, 12, 15*time.Second)
	names := messageNames(messages)

	assert.Len(t, names, 12, "valid messages on the same partition must survive an oversized neighbour")
	assert.NotContains(t, names, "same-partition-oversized")
	for i := 1; i <= 12; i++ {
		assert.Contains(t, names, fmt.Sprintf("same-partition-valid-%d", i))
	}
}

func TestConnector_SkipOversizedMessages_AllOversizedBatchDoesNotStallSlot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "oversized_events_stall"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "oversized.test.stall"
	cfg := oversizedConnectorConfig(
		"cdc_slot_oversized_stall",
		"cdc_publication_oversized_stall",
		tableName,
		topic,
		true,
		oversizedMaxBytes,
		5,
	)

	connector, err := cdc.NewConnector(ctx, cfg, oversizedPayloadHandler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(t, ctx, connector)

	for i := 1; i <= 5; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("oversized-only-%d", i), largePayload())
	}

	// Give the batch time to flush and ack skipped oversized events.
	time.Sleep(2 * time.Second)

	for i := 1; i <= 3; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("after-skip-valid-%d", i), smallPayload(i))
	}

	messages := readTopicMessages(t, topic, 3, 15*time.Second)
	names := messageNames(messages)

	assert.Len(t, names, 3, "connector must continue after a batch containing only oversized messages")
	for i := 1; i <= 3; i++ {
		assert.Contains(t, names, fmt.Sprintf("after-skip-valid-%d", i))
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable",
			Infra.PostgresHost,
			Infra.PostgresPort,
		),
	)
	require.NoError(t, err)
	return db
}

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	require.NoError(t, err)
}

func createOversizedEventsTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			payload TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`, tableName))
	require.NoError(t, err)
}

func insertOversizedEvent(t *testing.T, db *sql.DB, tableName, name, payload string) {
	t.Helper()

	_, err := db.Exec(
		fmt.Sprintf(`INSERT INTO %s (name, payload) VALUES ($1, $2)`, tableName),
		name,
		payload,
	)
	require.NoError(t, err)
}

func smallPayload(seed int) string {
	return fmt.Sprintf("small-payload-%d", seed)
}

func largePayload() string {
	return strings.Repeat("A", oversizedPayloadSize)
}

func oversizedConnectorConfig(
	slotName string,
	publicationName string,
	tableName string,
	topic string,
	skipOversized bool,
	maxMessageBytes string,
	batchSize int,
) config.Connector {
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)

	return config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              publicationName,
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            tableName,
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        slotName,
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				fmt.Sprintf("public.%s", tableName): topic,
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			SkipOversizedMessages:       skipOversized,
			MaxMessageBytes:             maxMessageBytes,
			ProducerBatchTickerDuration: 2 * time.Second,
			ProducerBatchSize:           batchSize,
		},
	}
}

func waitForConnectorReady(t *testing.T, ctx context.Context, connector cdc.Connector) {
	t.Helper()

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := connector.WaitUntilReady(readyCtx)
	require.NoError(t, err)
}

func readTopicMessages(t *testing.T, topic string, expectedCount int, timeout time.Duration) []map[string]interface{} {
	t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	require.NoError(t, reader.SetOffset(kafka.FirstOffset))

	msgCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	messages := make([]map[string]interface{}, 0, expectedCount)
	for len(messages) < expectedCount {
		message, err := reader.ReadMessage(msgCtx)
		require.NoError(t, err)

		var data map[string]interface{}
		require.NoError(t, json.Unmarshal(message.Value, &data))
		messages = append(messages, data)
	}

	return messages
}

func messageNames(messages []map[string]interface{}) []string {
	names := make([]string, 0, len(messages))
	for _, message := range messages {
		if name, ok := message["name"].(string); ok {
			names = append(names, name)
		}
	}
	return names
}

func oversizedPayloadHandler(msg *cdc.Message) []kafka.Message {
	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		key := kafkaKeyFromData(msg.NewData)
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

		key := kafkaKeyFromData(msg.OldData)
		return []kafka.Message{
			{
				Key:   []byte(key),
				Value: oldData,
			},
		}
	}

	return []kafka.Message{}
}

func samePartitionOversizedHandler(msg *cdc.Message) []kafka.Message {
	messages := oversizedPayloadHandler(msg)
	for i := range messages {
		messages[i].Key = []byte("fixed-partition-key")
	}
	return messages
}

func kafkaKeyFromData(data map[string]interface{}) string {
	id, ok := data["id"]
	if !ok {
		return ""
	}

	switch v := id.(type) {
	case int32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	default:
		return fmt.Sprint(v)
	}
}
