package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/mapper"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_Mapper(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "mapper_users"
	dropTable(t, db, tableName)
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL, password TEXT)`, tableName))
	require.NoError(t, err)

	topic := "mapper.test"
	cfg := oversizedConnectorConfig("cdc_slot_mapper", "cdc_publication_mapper", tableName, topic, false, "", 10)
	cfg.CDC.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}

	m := mapper.New(
		mapper.Apply(change.Exclude("password")),
		mapper.KeyFromColumns("id"),
		mapper.OperationHeader(),
		mapper.TombstoneOnDelete(),
	)

	connector, err := cdc.NewConnector(ctx, cfg, m)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	var id int
	require.NoError(t, db.QueryRow(
		fmt.Sprintf(`INSERT INTO %s (name, password) VALUES ('ada', 'secret') RETURNING id`, tableName),
	).Scan(&id))
	_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET name = 'grace' WHERE id = $1`, tableName), id)
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, tableName), id)
	require.NoError(t, err)

	records := readTopicRecords(t, topic, 3)

	ops := make([]string, 0, len(records))
	for _, record := range records {
		assert.Equal(t, strconv.Itoa(id), string(record.Key), "every change of the row must share its key")
		ops = append(ops, header(t, record, mapper.OperationHeaderKey))
	}
	assert.Equal(t, []string{"INSERT", "UPDATE", "DELETE"}, ops)

	names := make([]string, 0, 2)
	for _, record := range records[:2] {
		var data map[string]any
		require.NoError(t, json.Unmarshal(record.Value, &data))
		assert.NotContains(t, data, "password")
		names = append(names, fmt.Sprint(data["name"]))
	}
	assert.Equal(t, []string{"ada", "grace"}, names)
	assert.Empty(t, records[2].Value, "a delete must be sent as a tombstone")
}

func header(t *testing.T, record kafka.Message, key string) string {
	t.Helper()

	for _, h := range record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	require.Failf(t, "missing header", "header %q not found", key)
	return ""
}

func TestConnector_MapperFromConfig(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "mapper_config_users"
	dropTable(t, db, tableName)
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT, password TEXT)`, tableName))
	require.NoError(t, err)

	topic, deletedTopic := "mapper.config.test", "mapper.config.test.deleted"
	cfg := oversizedConnectorConfig("cdc_slot_mapper_config", "cdc_publication_mapper_config", tableName, topic, false, "", 10)
	cfg.CDC.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}
	cfg.Kafka.Mappings = map[string]config.Mapping{
		tableName: {
			Config: change.Config{
				Exclude: []string{"password"},
				Mask:    []string{"email"},
			},
			Key:               []string{"id"},
			Headers:           []string{"operation"},
			OperationTopics:   map[change.Op]string{change.Delete: deletedTopic},
			TombstoneOnDelete: true,
		},
	}

	handler, err := mapper.FromConfig(cfg.Kafka)
	require.NoError(t, err)

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	var id int
	require.NoError(t, db.QueryRow(
		fmt.Sprintf(`INSERT INTO %s (name, email, password) VALUES ('ada', 'ada@example.com', 'secret') RETURNING id`, tableName),
	).Scan(&id))
	_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET name = 'grace' WHERE id = $1`, tableName), id)
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, tableName), id)
	require.NoError(t, err)

	records := readTopicRecords(t, topic, 2)
	ops := make([]string, 0, len(records))
	for _, record := range records {
		assert.Equal(t, strconv.Itoa(id), string(record.Key))
		ops = append(ops, header(t, record, mapper.OperationHeaderKey))

		var data map[string]any
		require.NoError(t, json.Unmarshal(record.Value, &data))
		assert.NotContains(t, data, "password")
		assert.Equal(t, change.MaskValue, data["email"])
	}
	assert.Equal(t, []string{"INSERT", "UPDATE"}, ops)

	deleted := readTopicRecords(t, deletedTopic, 1)
	assert.Equal(t, strconv.Itoa(id), string(deleted[0].Key))
	assert.Equal(t, "DELETE", header(t, deleted[0], mapper.OperationHeaderKey))
	assert.Empty(t, deleted[0].Value, "a delete must be sent as a tombstone")
}
