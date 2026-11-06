package cdc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/logger"
	gokafka "github.com/segmentio/kafka-go"
)

const (
	OperationHeaderKey = "operation"
	TableHeaderKey     = "table"
	LSNHeaderKey       = "lsn"
	XIDHeaderKey       = "xid"

	keySeparator = ":"
)

// Apply runs target-independent steps from github.com/Trendyol/go-pq-cdc/change on the record's change.
func Apply(steps ...change.Step) Step {
	return func(r *Record) bool {
		return change.Run(r.Change, steps...)
	}
}

// KeyFromColumns sets the key from the row's columns, joined with ":" when there are several.
// The key stays empty when a column is missing.
func KeyFromColumns(cols ...string) Step {
	return func(r *Record) bool {
		row := r.Change.Row()
		parts := make([]string, 0, len(cols))
		for _, col := range cols {
			v, ok := row[col]
			if !ok {
				logger.Warn("dynamic mapper: key column missing", "table", r.Change.FullName(), "column", col)
				return true
			}
			parts = append(parts, fmt.Sprint(v))
		}
		r.Key = []byte(strings.Join(parts, keySeparator))
		return true
	}
}

// Header adds a header whose value is computed from the record.
func Header(key string, value func(r *Record) []byte) Step {
	return func(r *Record) bool {
		r.Headers = append(r.Headers, gokafka.Header{Key: key, Value: value(r)})
		return true
	}
}

// OperationHeader adds the operation (INSERT, UPDATE, DELETE, SNAPSHOT) as the "operation" header.
func OperationHeader() Step {
	return Header(OperationHeaderKey, func(r *Record) []byte { return []byte(r.Change.Op) })
}

// TableHeader adds schema.table as the "table" header.
func TableHeader() Step {
	return Header(TableHeaderKey, func(r *Record) []byte { return []byte(r.Change.FullName()) })
}

// LSNHeader adds the change's LSN in decimal as the "lsn" header.
func LSNHeader() Step {
	return Header(LSNHeaderKey, func(r *Record) []byte {
		return []byte(strconv.FormatUint(uint64(r.Change.LSN), 10))
	})
}

// XIDHeader adds the transaction id in decimal as the "xid" header.
func XIDHeader() Step {
	return Header(XIDHeaderKey, func(r *Record) []byte {
		return []byte(strconv.FormatUint(uint64(r.Change.Xid), 10))
	})
}

// Topic sends the message to topic instead of the table's topic.
func Topic(topic string) Step {
	return func(r *Record) bool {
		r.Topic = topic
		return true
	}
}

// TombstoneOnDelete sends deletes with a nil value, which compacted topics treat as a delete.
func TombstoneOnDelete() Step {
	return func(r *Record) bool {
		if r.Change.Op == change.Delete {
			r.Tombstone = true
		}
		return true
	}
}

// EnvelopeValue writes the value as {"op", "before", "after", "source"} instead of the bare row.
func EnvelopeValue() Step {
	return func(r *Record) bool {
		r.Encode = encodeEnvelopeJSON
		return true
	}
}

type envelope struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Op     change.Op      `json:"op"`
	Source envelopeSource `json:"source"`
}

type envelopeSource struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	LSN    uint64 `json:"lsn"`
	Xid    uint32 `json:"xid"`
}

func encodeEnvelopeJSON(c *change.Change) ([]byte, error) {
	return json.Marshal(envelope{
		Op:     c.Op,
		Before: c.Old,
		After:  c.New,
		Source: envelopeSource{
			Schema: c.Schema,
			Table:  c.Table,
			LSN:    uint64(c.LSN),
			Xid:    c.Xid,
		},
	})
}
