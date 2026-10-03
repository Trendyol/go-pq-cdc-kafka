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

const (
	lsnHeader = "lsn"
	xidHeader = "xid"
)

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

func TestConnector_MessageTransactionIDGroupsATransaction(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const parents, children = "tx_parents", "tx_children"
	dropTable(t, db, children)
	dropTable(t, db, parents)
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`, parents))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL, parent_id INT NOT NULL REFERENCES %s (id))`,
		children, parents,
	))
	require.NoError(t, err)

	topic := "tx.metadata.test"
	cfg := oversizedConnectorConfig("cdc_slot_tx", "cdc_publication_tx", parents, topic, false, "", 10)
	cfg.CDC.Publication.Tables = append(cfg.CDC.Publication.Tables, publication.Table{
		Name:            children,
		ReplicaIdentity: publication.ReplicaIdentityFull,
	})
	cfg.Kafka.TableTopicMapping["public."+children] = topic

	connector, err := cdc.NewConnector(ctx, cfg, metadataHandler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	tx, err := db.Begin()
	require.NoError(t, err)
	var parentID int
	require.NoError(t, tx.QueryRow(
		fmt.Sprintf(`INSERT INTO %s (name) VALUES ('parent-1') RETURNING id`, parents),
	).Scan(&parentID))
	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s (name, parent_id) VALUES ('child-1', $1)`, children), parentID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('parent-2')`, parents))
	require.NoError(t, err)

	records := readTopicRecords(t, topic, 3)

	names := make([]string, 0, len(records))
	xids := make([]uint64, 0, len(records))
	var previousLSN uint64
	for _, record := range records {
		var data map[string]any
		require.NoError(t, json.Unmarshal(record.Value, &data))
		names = append(names, fmt.Sprint(data["name"]))
		xids = append(xids, headerUint(t, record, xidHeader))

		lsn := headerUint(t, record, lsnHeader)
		assert.Greater(t, lsn, previousLSN, "LSN must increase in commit order")
		previousLSN = lsn
	}

	require.Equal(t, []string{"parent-1", "child-1", "parent-2"}, names)
	assert.NotZero(t, xids[0])
	assert.Equal(t, xids[0], xids[1], "changes of one transaction must share the transaction id")
	assert.NotEqual(t, xids[0], xids[2], "a new transaction must get a new transaction id")
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
			{Key: xidHeader, Value: []byte(strconv.FormatUint(uint64(msg.TransactionID), 10))},
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
