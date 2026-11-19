package cdc

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMessage_MatchesConstructors(t *testing.T) {
	const lsn pq.LSN = 0x16B374D848
	now := time.Now()
	oldRow := map[string]any{"id": int32(1), "name": "old"}
	newRow := map[string]any{"id": int32(1), "name": "new"}

	insert := &format.Insert{TableNamespace: "public", TableName: "users", Decoded: newRow, LSN: lsn, MessageTime: now}
	update := &format.Update{TableNamespace: "public", TableName: "users", OldDecoded: oldRow, NewDecoded: newRow, LSN: lsn, MessageTime: now}
	del := &format.Delete{TableNamespace: "public", TableName: "users", OldDecoded: oldRow, LSN: lsn, MessageTime: now}
	snapshot := &format.Snapshot{EventType: format.SnapshotEventTypeData, Schema: "public", Table: "users", Data: newRow, LSN: lsn, ServerTime: now}

	tests := []struct {
		message any
		want    *Message
		name    string
	}{
		{name: "insert", message: insert, want: NewInsertMessage(insert)},
		{name: "update", message: update, want: NewUpdateMessage(update)},
		{name: "delete", message: del, want: NewDeleteMessage(del)},
		{name: "snapshot row", message: snapshot, want: NewSnapshotMessage(snapshot)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want.TransactionID = 7
			got := newMessage(&replication.ListenerContext{Message: tt.message, Xid: 7})
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewMessage_SnapshotBoundariesStillDelivered(t *testing.T) {
	for _, eventType := range []format.SnapshotEventType{format.SnapshotEventTypeBegin, format.SnapshotEventTypeEnd} {
		t.Run(string(eventType), func(t *testing.T) {
			got := newMessage(&replication.ListenerContext{
				Message: &format.Snapshot{EventType: eventType, Schema: "public", Table: "users"},
			})

			require.NotNil(t, got)
			assert.Equal(t, SnapshotMessage, got.Type)
			assert.Nil(t, got.NewData)
		})
	}
}

func TestNewMessage_Unsupported(t *testing.T) {
	assert.Nil(t, newMessage(&replication.ListenerContext{Message: &format.Truncate{}}))
	assert.Nil(t, newMessage(&replication.ListenerContext{Message: &format.LogicalDecodingMessage{}}))
}

func TestMessage_Change(t *testing.T) {
	now := time.Now()
	msg := &Message{
		EventTime:      now,
		TableName:      "users",
		TableNamespace: "public",
		OldData:        map[string]any{"id": 1, "name": "old"},
		NewData:        map[string]any{"id": 1, "name": "new"},
		Type:           UpdateMessage,
		LSN:            42,
		TransactionID:  7,
	}

	assert.Equal(t, &change.Change{
		Op:        change.Update,
		Schema:    "public",
		Table:     "users",
		Old:       msg.OldData,
		New:       msg.NewData,
		LSN:       42,
		Xid:       7,
		EventTime: now,
	}, msg.Change())
}
