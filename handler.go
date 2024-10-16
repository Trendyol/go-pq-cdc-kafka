package cdc

import "github.com/segmentio/kafka-go"

type Handler func(event *Message) []kafka.Message
