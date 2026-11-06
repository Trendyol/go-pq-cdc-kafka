package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_DynamicMapper(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "dynamic_mapper_users"
	dropTable(t, db, tableName)
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT NOT NULL, password TEXT)`, tableName))
	require.NoError(t, err)

	topic := "dynamic.mapper.test"
	cfg := oversizedConnectorConfig("cdc_slot_dynamic", "cdc_publication_dynamic", tableName, topic, false, "", 10)
	cfg.CDC.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}

	mapper := cdc.NewDynamicMapper(
		cdc.Apply(change.Exclude("password")),
		cdc.KeyFromColumns("id"),
		cdc.OperationHeader(),
		cdc.TombstoneOnDelete(),
	)

	connector, err := cdc.NewConnector(ctx, cfg, mapper)
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
		ops = append(ops, header(t, record, cdc.OperationHeaderKey))
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
