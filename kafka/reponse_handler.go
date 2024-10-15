package kafka

import "github.com/segmentio/kafka-go"

type ResponseHandlerContext struct {
	Message *kafka.Message
	Err     error
}

type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
}
