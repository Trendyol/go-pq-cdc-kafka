package go_pq_cdc_kafka

import "github.com/segmentio/kafka-go"

type Handler func(event *Message) []kafka.Message
