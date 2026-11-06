package cdc

import (
	"encoding/json"

	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/logger"
	gokafka "github.com/segmentio/kafka-go"
)

// Record is the Kafka message a DynamicMapper builds for one change.
// Steps fill it in; the value is encoded after the last step.
type Record struct {
	// Change is a copy of the captured change, so steps can edit its rows freely.
	Change *change.Change
	// Encode turns the change into the message value. It defaults to the JSON of Change.Row().
	Encode func(c *change.Change) ([]byte, error)
	// Topic overrides the table's topic when set.
	Topic   string
	Key     []byte
	Headers []gokafka.Header
	// Tombstone sends the message with a nil value.
	Tombstone bool
}

// Step builds part of a Record. Returning false drops the change.
//
// Any function with this signature can be passed to NewDynamicMapper, so a new
// step is usable as soon as it is written. Steps from github.com/Trendyol/go-pq-cdc/change
// run through Apply.
type Step func(r *Record) bool

// NewDynamicMapper returns a Handler that runs steps in order on every change and
// sends one Kafka message per change that no step drops.
func NewDynamicMapper(steps ...Step) Handler {
	return func(msg *Message) []gokafka.Message {
		// Snapshot BEGIN and END events carry no row.
		if msg.Type.IsSnapshot() && msg.NewData == nil {
			return nil
		}

		r := &Record{Change: msg.Change().Clone(), Encode: encodeRowJSON}
		for _, step := range steps {
			if !step(r) {
				return nil
			}
		}

		var value []byte
		if !r.Tombstone {
			var err error
			value, err = r.Encode(r.Change)
			if err != nil {
				logger.Error("dynamic mapper: encode value", "table", r.Change.FullName(), "error", err)
				return nil
			}
		}

		return []gokafka.Message{{
			Topic:   r.Topic,
			Key:     r.Key,
			Value:   value,
			Headers: r.Headers,
		}}
	}
}

// DefaultMapper sends the row as JSON with an operation header and no key.
// The primary key is not part of the message, so choose the key with KeyFromColumns.
func DefaultMapper(msg *Message) []gokafka.Message {
	return defaultMapper(msg)
}

var defaultMapper = NewDynamicMapper(OperationHeader())

func encodeRowJSON(c *change.Change) ([]byte, error) {
	return json.Marshal(c.Row())
}
