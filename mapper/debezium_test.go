package mapper

import (
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebeziumValue(t *testing.T) {
	processed := time.UnixMilli(1_700_000_005_000)
	committed := time.UnixMilli(1_700_000_000_000)
	now = func() time.Time { return processed }
	t.Cleanup(func() { now = time.Now })

	tests := []struct {
		message *cdc.Message
		want    string
		name    string
	}{
		{
			name: "insert",
			message: &cdc.Message{
				Type: cdc.InsertMessage, TableNamespace: "public", TableName: "users",
				NewData: map[string]any{"id": 1}, LSN: 42, TransactionID: 9, EventTime: committed,
			},
			want: `{"before": null, "after": {"id": 1}, "op": "c", "ts_ms": 1700000005000,
				"source": {"connector": "postgresql", "schema": "public", "table": "users", "snapshot": "false",
					"ts_ms": 1700000000000, "lsn": 42, "txId": 9}}`,
		},
		{
			name: "update",
			message: &cdc.Message{
				Type: cdc.UpdateMessage, TableNamespace: "public", TableName: "users",
				OldData: map[string]any{"id": 1, "name": "old"}, NewData: map[string]any{"id": 1, "name": "new"},
				LSN: 43, TransactionID: 10, EventTime: committed,
			},
			want: `{"before": {"id": 1, "name": "old"}, "after": {"id": 1, "name": "new"}, "op": "u", "ts_ms": 1700000005000,
				"source": {"connector": "postgresql", "schema": "public", "table": "users", "snapshot": "false",
					"ts_ms": 1700000000000, "lsn": 43, "txId": 10}}`,
		},
		{
			name: "delete",
			message: &cdc.Message{
				Type: cdc.DeleteMessage, TableNamespace: "public", TableName: "users",
				OldData: map[string]any{"id": 1}, LSN: 44, TransactionID: 11, EventTime: committed,
			},
			want: `{"before": {"id": 1}, "after": null, "op": "d", "ts_ms": 1700000005000,
				"source": {"connector": "postgresql", "schema": "public", "table": "users", "snapshot": "false",
					"ts_ms": 1700000000000, "lsn": 44, "txId": 11}}`,
		},
		{
			name: "snapshot row",
			message: &cdc.Message{
				Type: cdc.SnapshotMessage, TableNamespace: "public", TableName: "users",
				NewData: map[string]any{"id": 1}, LSN: 40, EventTime: committed,
			},
			want: `{"before": null, "after": {"id": 1}, "op": "r", "ts_ms": 1700000005000,
				"source": {"connector": "postgresql", "schema": "public", "table": "users", "snapshot": "true",
					"ts_ms": 1700000000000, "lsn": 40, "txId": 0}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(DebeziumValue())(tt.message)

			require.Len(t, got, 1)
			assert.JSONEq(t, tt.want, string(got[0].Value))
		})
	}
}
