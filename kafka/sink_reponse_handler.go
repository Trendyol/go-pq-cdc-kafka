package kafka

import "github.com/segmentio/kafka-go"

type SinkResponseHandlerContext struct {
	Message *kafka.Message
	Err     error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}
