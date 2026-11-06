package mapper

import (
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func record(c *change.Change) *Record {
	return &Record{Change: c, Encode: encodeRowJSON}
}

func TestApply(t *testing.T) {
	r := record(&change.Change{Op: change.Insert, New: map[string]any{"id": 1, "password": "x"}})

	assert.True(t, Apply(change.Exclude("password"))(r))
	assert.Equal(t, map[string]any{"id": 1}, r.Change.New)
	assert.False(t, Apply(change.OnlyOps(change.Delete))(r))
}

func TestKeyFromColumns(t *testing.T) {
	tests := []struct {
		change *change.Change
		name   string
		want   []byte
		cols   []string
	}{
		{
			name:   "single column",
			change: &change.Change{Op: change.Insert, New: map[string]any{"id": int32(7)}},
			cols:   []string{"id"},
			want:   []byte("7"),
		},
		{
			name:   "several columns",
			change: &change.Change{Op: change.Update, New: map[string]any{"tenant": "eu", "id": int64(7)}},
			cols:   []string{"tenant", "id"},
			want:   []byte("eu:7"),
		},
		{
			name:   "delete reads the old row",
			change: &change.Change{Op: change.Delete, Old: map[string]any{"id": "abc"}},
			cols:   []string{"id"},
			want:   []byte("abc"),
		},
		{
			name:   "missing column leaves the key empty",
			change: &change.Change{Op: change.Insert, New: map[string]any{"id": 1}},
			cols:   []string{"id", "tenant"},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := record(tt.change)
			assert.True(t, KeyFromColumns(tt.cols...)(r))
			assert.Equal(t, tt.want, r.Key)
		})
	}
}

func TestHeaders(t *testing.T) {
	r := record(&change.Change{Op: change.Update, Schema: "sales", Table: "orders", LSN: 0x16B374D848, Xid: 12})

	for _, step := range []Step{OperationHeader(), TableHeader(), LSNHeader(), XIDHeader()} {
		require.True(t, step(r))
	}

	got := map[string]string{}
	for _, h := range r.Headers {
		got[h.Key] = string(h.Value)
	}
	assert.Equal(t, map[string]string{
		"operation": "UPDATE",
		"table":     "sales.orders",
		"lsn":       "97500059720",
		"xid":       "12",
	}, got)
}

func TestHeader_Custom(t *testing.T) {
	r := record(&change.Change{Op: change.Insert})

	Header("source", func(*Record) []byte { return []byte("pg") })(r)

	require.Len(t, r.Headers, 1)
	assert.Equal(t, "source", r.Headers[0].Key)
	assert.Equal(t, []byte("pg"), r.Headers[0].Value)
}

func TestTopic(t *testing.T) {
	r := record(&change.Change{Op: change.Insert})

	assert.True(t, Topic("users.v2")(r))
	assert.Equal(t, "users.v2", r.Topic)
}

func TestTombstoneOnDelete(t *testing.T) {
	mapper := New(KeyFromColumns("id"), TombstoneOnDelete())

	deleted := mapper(deleteMessage())
	inserted := mapper(insertMessage())

	require.Len(t, deleted, 1)
	assert.Equal(t, []byte("7"), deleted[0].Key)
	assert.Nil(t, deleted[0].Value)
	require.Len(t, inserted, 1)
	assert.NotNil(t, inserted[0].Value)
}

func TestEnvelopeValue(t *testing.T) {
	msg := &cdc.Message{
		Type:           cdc.UpdateMessage,
		TableNamespace: "public",
		TableName:      "users",
		OldData:        map[string]any{"id": 1, "name": "old"},
		NewData:        map[string]any{"id": 1, "name": "new"},
		LSN:            42,
		TransactionID:  9,
	}

	got := New(EnvelopeValue(), Apply(change.Exclude("id")))(msg)

	require.Len(t, got, 1)
	assert.JSONEq(t, `{
		"op": "UPDATE",
		"before": {"name": "old"},
		"after": {"name": "new"},
		"source": {"schema": "public", "table": "users", "lsn": 42, "xid": 9}
	}`, string(got[0].Value))
}

func TestEnvelopeValue_Insert(t *testing.T) {
	got := New(EnvelopeValue())(&cdc.Message{Type: cdc.InsertMessage, NewData: map[string]any{"id": 1}})

	assert.JSONEq(t, `{"op": "INSERT", "before": null, "after": {"id": 1}, "source": {"schema": "", "table": "", "lsn": 0, "xid": 0}}`, string(got[0].Value))
}
