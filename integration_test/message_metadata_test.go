package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lsnHeader = "lsn"

func TestConnector_MessageLSNIncreasesForARow(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "lsn_events"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "lsn.events.test"
	cfg := oversizedConnectorConfig("cdc_slot_lsn", "cdc_publication_lsn", tableName, topic, false, "", 10)
	cfg.CDC.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
	}

	connector, err := cdc.NewConnector(ctx, cfg, metadataHandler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	var id int
	require.NoError(t, db.QueryRow(
		fmt.Sprintf(`INSERT INTO %s (name) VALUES ('v1') RETURNING id`, tableName),
	).Scan(&id))
	for _, name := range []string{"v2", "v3"} {
		_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET name = $1 WHERE id = $2`, tableName), name, id)
		require.NoError(t, err)
	}

	records := readTopicRecords(t, topic, 3)

	var previous uint64
	for i, record := range records {
		var data map[string]any
		require.NoError(t, json.Unmarshal(record.Value, &data))
		assert.Equal(t, fmt.Sprintf("v%d", i+1), data["name"])

		lsn := headerUint(t, record, lsnHeader)
		assert.Greater(t, lsn, previous, "LSN must increase with every change to the row")
		previous = lsn
	}
}

func metadataHandler(msg *cdc.Message) []kafka.Message {
	data := msg.NewData
	if msg.Type.IsDelete() {
		data = msg.OldData
	}
	value, _ := json.Marshal(data)

	return []kafka.Message{{
		Key:   []byte(kafkaKeyFromData(data)),
		Value: value,
		Headers: []kafka.Header{
			{Key: lsnHeader, Value: []byte(strconv.FormatUint(uint64(msg.LSN), 10))},
		},
	}}
}

func readTopicRecords(t *testing.T, topic string, expectedCount int) []kafka.Message {
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	records := make([]kafka.Message, 0, expectedCount)
	for len(records) < expectedCount {
		record, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		records = append(records, record)
	}

	return records
}

func headerUint(t *testing.T, record kafka.Message, key string) uint64 {
	t.Helper()

	for _, h := range record.Headers {
		if h.Key == key {
			v, err := strconv.ParseUint(string(h.Value), 10, 64)
			require.NoError(t, err)
			return v
		}
	}

	require.Failf(t, "missing header", "header %q not found", key)
	return 0
}
