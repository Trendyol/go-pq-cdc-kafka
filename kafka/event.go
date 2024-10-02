package kafka

import (
	"fmt"
	"github.com/Trendyol/go-dcp-cdc-kafka/config"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	Topic   string
	Headers []kafka.Header
	Key     []byte
	Value   []byte
}

type Events []Event

func (es Events) KafkaMessages(config *config.Connector, tableNamespace, tableName string) []kafka.Message {
	messages := make([]kafka.Message, 0, len(es))
	for _, message := range es {
		messages = append(messages, kafka.Message{
			Topic:   getTopicName(config, tableNamespace, tableName, message.Topic),
			Key:     message.Key,
			Value:   message.Value,
			Headers: message.Headers,
		})
	}

	return messages
}

func getTopicName(config *config.Connector, tableNamespace, tableName, actionIndexName string) string {
	if actionIndexName != "" {
		return actionIndexName
	}

	indexName := config.Kafka.CollectionTopicMapping[fmt.Sprintf("%s.%s", tableNamespace, tableName)]
	if indexName == "" {
		panic(fmt.Sprintf("there is no index mapping for table: %s.%s on your configuration", tableNamespace, tableName))
	}

	return indexName
}
