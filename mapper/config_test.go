package mapper

import (
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func headerMap(msg gokafka.Message) map[string]string {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	return headers
}

func usersMapping() config.Mapping {
	return config.Mapping{
		Config: change.Config{
			Exclude: []string{"password"},
			Rename:  map[string]string{"id": "user_id"},
		},
		Key:               []string{"user_id"},
		Headers:           []string{"operation", "TABLE", "lsn", "xid"},
		OperationTopics:   map[change.Op]string{"delete": "users.deleted"},
		TombstoneOnDelete: true,
	}
}

func TestFromConfig_Mapping(t *testing.T) {
	handler, err := FromConfig(config.Kafka{
		TableTopicMapping: map[string]string{"public.users": "users"},
		Mappings:          map[string]config.Mapping{"users": usersMapping()},
	})
	require.NoError(t, err)

	insert := handler(insertMessage())
	require.Len(t, insert, 1)
	assert.Equal(t, "7", string(insert[0].Key))
	assert.Empty(t, insert[0].Topic, "inserts keep the table's topic")
	assert.Equal(t, map[string]any{"user_id": float64(7), "name": "ada"}, decodeValue(t, insert[0].Value))
	assert.Equal(t, map[string]string{"operation": "INSERT", "table": "public.users", "lsn": "42", "xid": "9"}, headerMap(insert[0]))

	del := handler(deleteMessage())
	require.Len(t, del, 1)
	assert.Equal(t, "7", string(del[0].Key))
	assert.Equal(t, "users.deleted", del[0].Topic)
	assert.Nil(t, del[0].Value)
}

func TestFromConfig_Filters(t *testing.T) {
	handler, err := FromConfig(config.Kafka{Mappings: map[string]config.Mapping{
		"users": {Config: change.Config{Operations: []change.Op{change.Insert}}},
	}})
	require.NoError(t, err)

	assert.Len(t, handler(insertMessage()), 1)
	assert.Empty(t, handler(deleteMessage()))
}

func TestFromConfig_TopicAndEnvelope(t *testing.T) {
	handler, err := FromConfig(config.Kafka{Mappings: map[string]config.Mapping{
		"users": {Topic: "all-users", Value: "Envelope"},
	}})
	require.NoError(t, err)

	got := handler(insertMessage())
	require.Len(t, got, 1)
	assert.Equal(t, "all-users", got[0].Topic)
	assert.Equal(t, "INSERT", decodeValue(t, got[0].Value)["op"])
}

func TestFromConfig_Fallback(t *testing.T) {
	orders := insertMessage()
	orders.TableName = "orders"

	handler, err := FromConfig(config.Kafka{Mappings: map[string]config.Mapping{"users": {Key: []string{"id"}}}})
	require.NoError(t, err)
	assert.Equal(t, DefaultMapper(orders), handler(orders), "tables without a mapping use DefaultMapper")

	custom := func(*cdc.Message) []gokafka.Message { return []gokafka.Message{{Topic: "legacy"}} }
	handler, err = FromConfig(config.Kafka{Mappings: map[string]config.Mapping{"users": {}}}, WithFallback(custom))
	require.NoError(t, err)
	assert.Equal(t, "legacy", handler(orders)[0].Topic)
	assert.NotEqual(t, "legacy", handler(insertMessage())[0].Topic, "a mapped table does not use the fallback")
}

func TestFromConfig_WithSteps(t *testing.T) {
	source := Header("source", func(*Record) []byte { return []byte("users-db") })
	upperName := Apply(func(c *change.Change) bool {
		c.New["name"] = "ADA"
		return true
	})

	handler, err := FromConfig(
		config.Kafka{Mappings: map[string]config.Mapping{"public.users": {Config: change.Config{Exclude: []string{"password"}}}}},
		WithSteps("users", upperName, source),
	)
	require.NoError(t, err)

	got := handler(insertMessage())
	require.Len(t, got, 1)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ADA"}, decodeValue(t, got[0].Value))
	assert.Equal(t, "users-db", headerMap(got[0])["source"])
}

func TestFromConfig_Errors(t *testing.T) {
	tests := []struct {
		kafka config.Kafka
		name  string
		opts  []Option
		want  []string
	}{
		{
			name: "table without a topic",
			kafka: config.Kafka{
				TableTopicMapping: map[string]string{"public.orders": "orders"},
				Mappings:          map[string]config.Mapping{"users": {}},
			},
			want: []string{"public.users: table is not in tableTopicMapping"},
		},
		{
			name:  "same table twice",
			kafka: config.Kafka{Mappings: map[string]config.Mapping{"users": {}, "public.users": {}}},
			want:  []string{`"public.users" and "users" name the same table`},
		},
		{
			name:  "column config",
			kafka: config.Kafka{Mappings: map[string]config.Mapping{"users": {Config: change.Config{Operations: []change.Op{"UPSERT"}}}}},
			want:  []string{`unknown operation "UPSERT"`},
		},
		{
			name: "kafka fields",
			kafka: config.Kafka{Mappings: map[string]config.Mapping{"users": {
				Headers:         []string{"partition"},
				OperationTopics: map[change.Op]string{"UPSERT": "x", change.Delete: ""},
				Value:           "avro",
			}}},
			want: []string{`unknown header "partition"`, `operationTopics: unknown operation "UPSERT"`, "empty topic for DELETE", `unknown value "avro"`},
		},
		{
			name:  "steps for a table without a mapping",
			kafka: config.Kafka{Mappings: map[string]config.Mapping{"users": {}}},
			opts:  []Option{WithSteps("orders", OperationHeader())},
			want:  []string{"WithSteps public.orders: table has no mapping"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := FromConfig(tt.kafka, tt.opts...)
			require.Error(t, err)
			assert.Nil(t, handler)
			for _, want := range tt.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

func TestKeyColumnRemoved(t *testing.T) {
	tests := []struct {
		name string
		col  string
		cfg  change.Config
		want bool
	}{
		{name: "kept", col: "id", cfg: change.Config{Exclude: []string{"password"}}},
		{name: "excluded", col: "id", cfg: change.Config{Exclude: []string{"id"}}, want: true},
		{name: "not included", col: "id", cfg: change.Config{Include: []string{"name"}}, want: true},
		{name: "included", col: "id", cfg: change.Config{Include: []string{"id"}}},
		{name: "renamed away", col: "id", cfg: change.Config{Rename: map[string]string{"id": "user_id"}}, want: true},
		{name: "renamed to", col: "user_id", cfg: change.Config{Include: []string{"id"}, Rename: map[string]string{"id": "user_id"}}},
		{name: "renamed from an excluded column", col: "user_id", cfg: change.Config{Exclude: []string{"id"}, Rename: map[string]string{"id": "user_id"}}, want: true},
		{name: "inserted", col: "tenant", cfg: change.Config{Include: []string{"id"}, InsertFields: map[string]any{"tenant": "t1"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, keyColumnRemoved(tt.cfg, tt.col))
		})
	}

	_, err := FromConfig(config.Kafka{Mappings: map[string]config.Mapping{
		"users": {Config: change.Config{Exclude: []string{"id"}}, Key: []string{"id"}},
	}})
	assert.ErrorContains(t, err, `key column "id" is removed`)
}

func TestFromConfig_FromYAML(t *testing.T) {
	var cfg config.Connector
	require.NoError(t, yaml.Unmarshal([]byte(`
kafka:
  tableTopicMapping:
    public.users: users
  mappings:
    public.users:
      exclude: [password]
      key: [id]
      headers: [operation]
      operationTopics:
        DELETE: users.deleted
      tombstoneOnDelete: true
`), &cfg))

	assert.Equal(t, config.Mapping{
		Config:            change.Config{Exclude: []string{"password"}},
		Key:               []string{"id"},
		Headers:           []string{"operation"},
		OperationTopics:   map[change.Op]string{change.Delete: "users.deleted"},
		TombstoneOnDelete: true,
	}, cfg.Kafka.Mappings["public.users"])

	handler, err := FromConfig(cfg.Kafka)
	require.NoError(t, err)
	assert.Equal(t, "users.deleted", handler(deleteMessage())[0].Topic)
}

func TestFromConfig_HypertableChunkUsesParentMapping(t *testing.T) {
	timescaledb.HyperTables.Store("_timescaledb_internal._hyper_1_1_chunk", "public.users")
	t.Cleanup(func() { timescaledb.HyperTables.Delete("_timescaledb_internal._hyper_1_1_chunk") })

	handler, err := FromConfig(config.Kafka{Mappings: map[string]config.Mapping{"users": {Key: []string{"id"}}}})
	require.NoError(t, err)

	chunk := insertMessage()
	chunk.TableNamespace, chunk.TableName = "_timescaledb_internal", "_hyper_1_1_chunk"
	assert.Equal(t, "7", string(handler(chunk)[0].Key))
}
