package metadata

import (
	gKafka "github.com/Trendyol/go-dcp-cdc-kafka/kafka"
	"github.com/segmentio/kafka-go"
)

type kafkaMetadata struct {
	kafkaClient gKafka.Client
	writer      *kafka.Writer
	topic       string
}
