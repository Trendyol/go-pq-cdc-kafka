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

// ResponseHandler receives per-message delivery results.
//
// All callbacks run synchronously on the producer flush path, while the flush
// lock is held and before the replication position is acknowledged. They must
// be fast and must not block: a slow callback stalls flushing, acking and
// replication reading. Message pointers are only valid for the duration of the
// call. A panic inside a callback prevents the ack.
type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
}

// BatchResponseHandler is an optional extension of ResponseHandler. When the
// handler passed to WithResponseHandler also implements it, OnBatchSuccess is
// called once per flush with every message written to Kafka successfully, in
// producer order, instead of one OnSuccess call per message. OnError is still
// called per message. Use it to do a single round trip (e.g. one DELETE ...
// WHERE id = ANY($1)) per batch instead of one per message. The same
// non-blocking contract as ResponseHandler applies; the slice is only valid for
// the duration of the call.
type BatchResponseHandler interface {
	OnBatchSuccess(messages []*kafka.Message)
}

type DefaultResponseHandler struct {
	SkipOversizedMessages bool
}

func NewDefaultResponseHandler(skipOversizedMessages bool) *DefaultResponseHandler {
	return &DefaultResponseHandler{SkipOversizedMessages: skipOversizedMessages}
}

func (drh *DefaultResponseHandler) OnSuccess(_ *ResponseHandlerContext) {}

func (drh *DefaultResponseHandler) OnError(ctx *ResponseHandlerContext) {
	if drh.SkipOversizedMessages && IsMessageTooLarge(ctx.Err) {
		if ctx.Message != nil {
			logger.Error("oversized kafka message skipped",
				"topic", ctx.Message.Topic,
				"key", string(ctx.Message.Key),
				"valueBytes", len(ctx.Message.Value),
				"error", ctx.Err,
			)
		} else {
			logger.Error("oversized kafka message skipped", "error", ctx.Err)
		}
		return
	}

	if isFatalError(ctx.Err) {
		logger.Error("permanent error on kafka while flush messages", "error", ctx.Err)
		panic(fmt.Errorf("permanent error on Kafka side %w", ctx.Err))
	}
	logger.Error("batch producer flush", "error", ctx.Err)
}

func IsMessageTooLarge(err error) bool {
	if errors.Is(err, kafka.MessageSizeTooLarge) {
		return true
	}

	var tooLarge kafka.MessageTooLargeError
	return errors.As(err, &tooLarge)
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
