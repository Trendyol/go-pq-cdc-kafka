package mapper

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/change"
)

// now is replaced in tests.
var now = time.Now

var debeziumOps = map[change.Op]string{
	change.Insert:   "c",
	change.Update:   "u",
	change.Delete:   "d",
	change.Snapshot: "r",
}

// DebeziumValue writes the value in the shape of a Debezium PostgreSQL change event
// payload (JSON converter, schemas disabled), so consumers written for Debezium can read it:
//
//	{"before": ..., "after": ..., "source": {...}, "op": "c|u|d|r", "ts_ms": ...}
//
// source carries connector, schema, table, txId, lsn, snapshot and ts_ms (the change's event time).
// Fields this library does not know, such as the database and server name, are left out.
func DebeziumValue() Step {
	return func(r *Record) bool {
		r.Encode = encodeDebeziumJSON
		return true
	}
}

type debeziumEvent struct {
	Before      map[string]any `json:"before"`
	After       map[string]any `json:"after"`
	Op          string         `json:"op"`
	Source      debeziumSource `json:"source"`
	TimestampMs int64          `json:"ts_ms"`
}

type debeziumSource struct {
	Connector   string `json:"connector"`
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	Snapshot    string `json:"snapshot"`
	TimestampMs int64  `json:"ts_ms"`
	LSN         uint64 `json:"lsn"`
	TxID        uint32 `json:"txId"`
}

func encodeDebeziumJSON(c *change.Change) ([]byte, error) {
	return json.Marshal(debeziumEvent{
		Before:      c.Old,
		After:       c.New,
		Op:          debeziumOps[c.Op],
		TimestampMs: now().UnixMilli(),
		Source: debeziumSource{
			Connector:   "postgresql",
			Schema:      c.Schema,
			Table:       c.Table,
			Snapshot:    strconv.FormatBool(c.Op == change.Snapshot),
			TimestampMs: c.EventTime.UnixMilli(),
			LSN:         uint64(c.LSN),
			TxID:        c.Xid,
		},
	})
}
