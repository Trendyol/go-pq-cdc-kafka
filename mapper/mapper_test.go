package mapper

import (
	"encoding/json"
	"errors"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc/change"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertMessage() *cdc.Message {
	return &cdc.Message{
		Type:           cdc.InsertMessage,
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(7), "name": "ada", "password": "secret"},
		LSN:            42,
		TransactionID:  9,
	}
}

func deleteMessage() *cdc.Message {
	return &cdc.Message{
		Type:           cdc.DeleteMessage,
		TableNamespace: "public",
		TableName:      "users",
		OldData:        map[string]any{"id": int32(7), "name": "ada"},
		LSN:            43,
		TransactionID:  10,
	}
}

func decodeValue(t *testing.T, value []byte) map[string]any {
	t.Helper()
	var got map[string]any
	require.NoError(t, json.Unmarshal(value, &got))
	return got
}

func TestNew_NoStepsWritesRowJSON(t *testing.T) {
	got := New()(insertMessage())

	require.Len(t, got, 1)
	assert.Nil(t, got[0].Key)
	assert.Empty(t, got[0].Topic)
	assert.Empty(t, got[0].Headers)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ada", "password": "secret"}, decodeValue(t, got[0].Value))
}

func TestNew_DeleteWritesOldRow(t *testing.T) {
	got := New()(deleteMessage())

	require.Len(t, got, 1)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ada"}, decodeValue(t, got[0].Value))
}

func TestNew_FullPipeline(t *testing.T) {
	mapper := New(
		Apply(
			change.OnlyOps(change.Insert, change.Update, change.Delete),
			change.Exclude("password"),
		),
		KeyFromColumns("id"),
		OperationHeader(),
		TableHeader(),
		LSNHeader(),
		XIDHeader(),
		Topic("users.v2"),
	)

	got := mapper(insertMessage())

	require.Len(t, got, 1)
	assert.Equal(t, "users.v2", got[0].Topic)
	assert.Equal(t, []byte("7"), got[0].Key)
	assert.Equal(t, []gokafka.Header{
		{Key: "operation", Value: []byte("INSERT")},
		{Key: "table", Value: []byte("public.users")},
		{Key: "lsn", Value: []byte("42")},
		{Key: "xid", Value: []byte("9")},
	}, got[0].Headers)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ada"}, decodeValue(t, got[0].Value))
}

func TestNew_DroppedChangeSendsNothing(t *testing.T) {
	called := false
	mapper := New(
		Apply(change.OnlyOps(change.Delete)),
		func(*Record) bool {
			called = true
			return true
		},
	)

	assert.Nil(t, mapper(insertMessage()))
	assert.False(t, called)
}

func TestNew_LeavesMessageUntouched(t *testing.T) {
	msg := insertMessage()

	New(Apply(change.Exclude("password"), change.Rename(map[string]string{"name": "full_name"})))(msg)

	assert.Equal(t, map[string]any{"id": int32(7), "name": "ada", "password": "secret"}, msg.NewData)
}

func TestNew_StepsRunInOrder(t *testing.T) {
	keyThenRename := New(KeyFromColumns("name"), Apply(change.Rename(map[string]string{"name": "full_name"})))
	renameThenKey := New(Apply(change.Rename(map[string]string{"name": "full_name"})), KeyFromColumns("name"))

	assert.Equal(t, []byte("ada"), keyThenRename(insertMessage())[0].Key)
	assert.Nil(t, renameThenKey(insertMessage())[0].Key)
}

func TestNew_CustomStep(t *testing.T) {
	upperName := func(r *Record) bool {
		r.Change.New["name"] = "ADA"
		return true
	}

	got := New(upperName)(insertMessage())

	assert.Equal(t, "ADA", decodeValue(t, got[0].Value)["name"])
}

func TestNew_EncodeErrorSendsNothing(t *testing.T) {
	failing := func(r *Record) bool {
		r.Encode = func(*change.Change) ([]byte, error) { return nil, errors.New("boom") }
		return true
	}

	assert.Nil(t, New(failing)(insertMessage()))
}

func TestNew_SkipsSnapshotBoundaries(t *testing.T) {
	assert.Nil(t, New()(&cdc.Message{Type: cdc.SnapshotMessage}))
}

func TestNew_SnapshotRow(t *testing.T) {
	got := New(OperationHeader())(&cdc.Message{Type: cdc.SnapshotMessage, NewData: map[string]any{"id": 1}})

	require.Len(t, got, 1)
	assert.Equal(t, []byte("SNAPSHOT"), got[0].Headers[0].Value)
}

func TestDefaultMapper(t *testing.T) {
	got := DefaultMapper(insertMessage())

	require.Len(t, got, 1)
	assert.Nil(t, got[0].Key)
	assert.Equal(t, []gokafka.Header{{Key: "operation", Value: []byte("INSERT")}}, got[0].Headers)
	assert.Equal(t, "ada", decodeValue(t, got[0].Value)["name"])
}
