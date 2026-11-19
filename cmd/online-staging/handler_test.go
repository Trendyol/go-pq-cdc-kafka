package main

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/stretchr/testify/require"
)

func TestOnlineStagingCanaryHandlerTransformsActualImagesAndExcludesVolatileClock(t *testing.T) {
	for _, kind := range []cdc.MessageType{cdc.SnapshotMessage, cdc.InsertMessage, cdc.UpdateMessage, cdc.DeleteMessage} {
		t.Run(string(kind), func(t *testing.T) {
			before := map[string]any{"id": int32(7), "value": "before"}
			after := map[string]any{"id": int32(7), "value": "after"}
			message := &cdc.Message{TableNamespace: "public", TableName: "items", Type: kind, EventTime: time.Now()}
			if kind == cdc.UpdateMessage || kind == cdc.DeleteMessage {
				message.OldData = before
			}
			if kind != cdc.DeleteMessage {
				message.NewData = after
			}
			var failed atomic.Bool
			records := canaryHandler("id", &failed)(message)
			require.False(t, failed.Load())
			require.Len(t, records, 1)
			var payload canaryPayload
			require.NoError(t, json.Unmarshal(records[0].Value, &payload))
			require.Equal(t, int64(7), payload.Key)
			require.Equal(t, 1, payload.Version)
			require.Equal(t, cdc.OnlineMessageKey("public", "items", 7), records[0].Key)
			if kind == cdc.SnapshotMessage {
				require.Equal(t, "READ", payload.Operation)
			} else {
				require.Equal(t, string(kind), payload.Operation)
			}
			if message.OldData == nil {
				require.Nil(t, payload.Before)
			} else {
				require.Equal(t, "before", payload.Before["value"])
			}
			if message.NewData == nil {
				require.Nil(t, payload.After)
			} else {
				require.Equal(t, "after", payload.After["value"])
			}
			first := records[0].Value
			message.EventTime = message.EventTime.Add(time.Hour)
			require.Equal(t, first, canaryHandler("id", &failed)(message)[0].Value, "sender clock cannot define deterministic replay content")
		})
	}
}

func TestOnlineStagingCanaryMarshalAndKeyFailuresReturnExplicitInvalidRecord(t *testing.T) {
	for name, image := range map[string]map[string]any{
		"unsupported source key": {"id": "not-an-integer"},
		"marshal failure":        {"id": int64(7), "unsupported": make(chan struct{})},
	} {
		t.Run(name, func(t *testing.T) {
			var failed atomic.Bool
			records := canaryHandler("id", &failed)(&cdc.Message{Type: cdc.InsertMessage, NewData: image})
			require.True(t, failed.Load())
			require.Len(t, records, 1, "never silently return nil/empty filtering")
			require.Empty(t, records[0].Key)
			require.Empty(t, records[0].Value, "protected actual delivery must reject before write/ACK")
		})
	}
}
