package go_pq_cdc_kafka

import (
	"github.com/Trendyol/go-dcp-cdc-kafka/kafka"
)

type Handler func(event *Message) []kafka.Event
