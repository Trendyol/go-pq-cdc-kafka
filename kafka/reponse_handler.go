package kafka

import (
	"fmt"
	"io"
	"syscall"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type ResponseHandlerContext struct {
	Message *kafka.Message
	Err     error
}

type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
}

type DefaultResponseHandler struct{}

func (drh *DefaultResponseHandler) OnSuccess(_ *ResponseHandlerContext) {}
func (drh *DefaultResponseHandler) OnError(ctx *ResponseHandlerContext) {
	if isFatalError(ctx.Err) {
		logger.Error("permanent error on kafka while flush messages", "error", ctx.Err)
		panic(fmt.Errorf("permanent error on Kafka side %w", ctx.Err))
	}
	logger.Error("batch producer flush", "error", ctx.Err)
}

func isFatalError(err error) bool {
	var e kafka.Error
	ok := errors.As(err, &e)
	if ok && errors.Is(err, kafka.UnknownTopicOrPartition) {
		return true
	}
	if (ok && e.Temporary()) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return false
	}
	return true
}
