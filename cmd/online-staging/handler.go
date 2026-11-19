package main

import (
	"encoding/json"
	"errors"
	"sync/atomic"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	gokafka "github.com/segmentio/kafka-go"
)

var errTransform = errors.New("transform")

type canaryPayload struct {
	Before    map[string]any `json:"before"`
	After     map[string]any `json:"after"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	Operation string         `json:"operation"`
	KeyColumn string         `json:"keyColumn"`
	Key       int64          `json:"key"`
	Version   int            `json:"version"`
}

func canaryHandler(keyColumn string, failed *atomic.Bool) cdc.Handler {
	return func(message *cdc.Message) []gokafka.Message {
		record, err := transformCanary(message, keyColumn)
		if err != nil {
			failed.Store(true)
			// Void Handler APIs cannot return an error. This private bridge returns
			// one explicitly invalid record: protected delivery refuses it before
			// writing/ACK, and the lifecycle reports a bounded transform category.
			return []gokafka.Message{{}}
		}
		return []gokafka.Message{record}
	}
}

func transformCanary(message *cdc.Message, keyColumn string) (gokafka.Message, error) {
	image := message.NewData
	operation := string(message.Type)
	switch message.Type {
	case cdc.SnapshotMessage:
		operation = "READ"
	case cdc.InsertMessage, cdc.UpdateMessage:
	case cdc.DeleteMessage:
		image = message.OldData
	default:
		return gokafka.Message{}, errTransform
	}
	var key int64
	switch value := image[keyColumn].(type) {
	case int16:
		key = int64(value)
	case int32:
		key = int64(value)
	case int64:
		key = value
	default:
		return gokafka.Message{}, errTransform
	}
	// Sender observation time is deliberately not deterministic replay content.
	payload := canaryPayload{Version: 1, Schema: message.TableNamespace, Table: message.TableName, Operation: operation, KeyColumn: keyColumn, Key: key, Before: message.OldData, After: message.NewData}
	data, err := json.Marshal(payload)
	if err != nil {
		return gokafka.Message{}, errTransform
	}
	return gokafka.Message{Key: cdc.OnlineMessageKey(message.TableNamespace, message.TableName, key), Value: data, Headers: []gokafka.Header{{Key: "content-type", Value: []byte("application/vnd.trendyol.online-staging+json")}}}, nil
}
