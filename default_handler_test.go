package cdc

import (
	"encoding/json"
	"testing"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultHandler_InsertStringifiesKey(t *testing.T) {
	h := defaultHandler(config.MapperConfig{KeyField: "id"})
	msgs := h(&Message{
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(9), "name": "n"},
		Type:           InsertMessage,
	})
	require.Len(t, msgs, 1)
	assert.Equal(t, "9", string(msgs[0].Key))

	var body map[string]any
	require.NoError(t, json.Unmarshal(msgs[0].Value, &body))
	assert.Equal(t, "INSERT", body["operation"])
	assert.Equal(t, "n", body["name"])
	assert.Equal(t, "operation", msgs[0].Headers[0].Key)
	assert.Equal(t, "cdc", string(msgs[0].Headers[2].Value))
}

func TestDefaultHandler_DeleteUsesOldData(t *testing.T) {
	h := defaultHandler(config.MapperConfig{KeyField: "id"})
	msgs := h(&Message{
		TableNamespace: "public",
		TableName:      "users",
		OldData:        map[string]any{"id": int32(4)},
		Type:           DeleteMessage,
	})
	require.Len(t, msgs, 1)
	assert.Equal(t, "4", string(msgs[0].Key))
	assert.Equal(t, "DELETE", string(msgs[0].Headers[0].Value))
}

func TestDefaultHandler_SnapshotHeader(t *testing.T) {
	h := defaultHandler(config.MapperConfig{KeyField: "id"})
	msgs := h(&Message{
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(1)},
		Type:           SnapshotMessage,
	})
	require.Len(t, msgs, 1)
	assert.Equal(t, "initial-snapshot", string(msgs[0].Headers[2].Value))
	assert.Equal(t, "public.users", string(msgs[0].Headers[1].Value))
}

func TestDefaultHandler_TableKeyMapping(t *testing.T) {
	h := defaultHandler(config.MapperConfig{
		KeyField:        "id",
		TableKeyMapping: map[string]string{"public.users": "email"},
	})
	msgs := h(&Message{
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(1), "email": "a@b.c"},
		Type:           InsertMessage,
	})
	require.Len(t, msgs, 1)
	assert.Equal(t, "a@b.c", string(msgs[0].Key))
}

func TestDefaultHandler_DoesNotMutateOriginal(t *testing.T) {
	msg := &Message{
		TableNamespace: "public",
		TableName:      "users",
		NewData:        map[string]any{"id": int32(1)},
		Type:           InsertMessage,
	}
	_ = defaultHandler(config.MapperConfig{KeyField: "id"})(msg)
	_, ok := msg.NewData["operation"]
	assert.False(t, ok)
}
