package cdc

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/segmentio/kafka-go"
)

func defaultHandler(cfg config.MapperConfig) Handler {
	return func(msg *Message) []kafka.Message {
		payload := msg.NewData
		if msg.Type.IsDelete() {
			payload = msg.OldData
		}
		if payload == nil {
			return nil
		}

		data := maps.Clone(payload)
		data["operation"] = string(msg.Type)

		raw, err := json.Marshal(data)
		if err != nil {
			return nil
		}

		table := msg.TableNamespace + "." + msg.TableName
		source := "cdc"
		if msg.Type.IsSnapshot() {
			source = "initial-snapshot"
		}

		return []kafka.Message{{
			Key:   kafkaKey(cfg, table, data),
			Value: raw,
			Headers: []kafka.Header{
				{Key: "operation", Value: []byte(msg.Type)},
				{Key: "table", Value: []byte(table)},
				{Key: "source", Value: []byte(source)},
			},
		}}
	}
}

func kafkaKey(cfg config.MapperConfig, table string, data map[string]any) []byte {
	field := cfg.KeyField
	if mapped, ok := cfg.TableKeyMapping[table]; ok && mapped != "" {
		field = mapped
	}
	if field == "" {
		return nil
	}
	v, ok := data[field]
	if !ok || v == nil {
		return nil
	}
	return []byte(fmt.Sprintf("%v", v))
}
