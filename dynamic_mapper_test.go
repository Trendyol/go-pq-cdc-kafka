package cdc

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Trendyol/go-pq-cdc/change"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertMessage() *Message {
	return &Message{
		Type:           InsertMessage,
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(7), "name": "ada", "password": "secret"},
		LSN:            42,
		TransactionID:  9,
	}
}

func deleteMessage() *Message {
	return &Message{
		Type:           DeleteMessage,
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

func TestDynamicMapper_NoStepsWritesRowJSON(t *testing.T) {
	got := NewDynamicMapper()(insertMessage())

	require.Len(t, got, 1)
	assert.Nil(t, got[0].Key)
	assert.Empty(t, got[0].Topic)
	assert.Empty(t, got[0].Headers)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ada", "password": "secret"}, decodeValue(t, got[0].Value))
}

func TestDynamicMapper_DeleteWritesOldRow(t *testing.T) {
	got := NewDynamicMapper()(deleteMessage())

	require.Len(t, got, 1)
	assert.Equal(t, map[string]any{"id": float64(7), "name": "ada"}, decodeValue(t, got[0].Value))
}

func TestDynamicMapper_FullPipeline(t *testing.T) {
	mapper := NewDynamicMapper(
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

func TestDynamicMapper_DroppedChangeSendsNothing(t *testing.T) {
	called := false
	mapper := NewDynamicMapper(
		Apply(change.OnlyOps(change.Delete)),
		func(*Record) bool {
			called = true
			return true
		},
	)

	assert.Nil(t, mapper(insertMessage()))
	assert.False(t, called)
}

func TestDynamicMapper_LeavesMessageUntouched(t *testing.T) {
	msg := insertMessage()

	NewDynamicMapper(Apply(change.Exclude("password"), change.Rename(map[string]string{"name": "full_name"})))(msg)

	assert.Equal(t, map[string]any{"id": int32(7), "name": "ada", "password": "secret"}, msg.NewData)
}

func TestDynamicMapper_StepsRunInOrder(t *testing.T) {
	keyThenRename := NewDynamicMapper(KeyFromColumns("name"), Apply(change.Rename(map[string]string{"name": "full_name"})))
	renameThenKey := NewDynamicMapper(Apply(change.Rename(map[string]string{"name": "full_name"})), KeyFromColumns("name"))

	assert.Equal(t, []byte("ada"), keyThenRename(insertMessage())[0].Key)
	assert.Nil(t, renameThenKey(insertMessage())[0].Key)
}

func TestDynamicMapper_CustomStep(t *testing.T) {
	upperName := func(r *Record) bool {
		r.Change.New["name"] = "ADA"
		return true
	}

	got := NewDynamicMapper(upperName)(insertMessage())

	assert.Equal(t, "ADA", decodeValue(t, got[0].Value)["name"])
}

func TestDynamicMapper_EncodeErrorSendsNothing(t *testing.T) {
	failing := func(r *Record) bool {
		r.Encode = func(*change.Change) ([]byte, error) { return nil, errors.New("boom") }
		return true
	}

	assert.Nil(t, NewDynamicMapper(failing)(insertMessage()))
}

func TestDynamicMapper_SkipsSnapshotBoundaries(t *testing.T) {
	assert.Nil(t, NewDynamicMapper()(&Message{Type: SnapshotMessage}))
}

func TestDynamicMapper_SnapshotRow(t *testing.T) {
	got := NewDynamicMapper(OperationHeader())(&Message{Type: SnapshotMessage, NewData: map[string]any{"id": 1}})

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
