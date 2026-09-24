package producer

import (
	"context"
	"sync"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"

	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	gokafka "github.com/segmentio/kafka-go"
)

type Batch struct {
	responseHandler       kafka.ResponseHandler
	metric                Metric
	batchTicker           *time.Ticker
	Writer                *gokafka.Writer
	lastAckCtx            *replication.ListenerContext
	messages              []gokafka.Message
	batchTickerDuration   time.Duration
	batchLimit            int
	batchBytes            int64
	maxMessageBytes       int64
	currentMessageBytes   int64
	flushLock             sync.Mutex
	hasPendingMessages    bool
	skipOversizedMessages bool
}

func newBatch(
	batchTime time.Duration,
	writer *gokafka.Writer,
	batchLimit int,
	batchBytes int64,
	maxMessageBytes int64,
	skipOversizedMessages bool,
	responseHandler kafka.ResponseHandler,
	slotName string,
	pqCDC cdc.Connector,
) *Batch {
	batch := &Batch{
		batchTickerDuration:   batchTime,
		batchTicker:           time.NewTicker(batchTime),
		metric:                NewMetric(pqCDC, slotName),
		messages:              make([]gokafka.Message, 0, batchLimit),
		Writer:                writer,
		batchLimit:            batchLimit,
		batchBytes:            batchBytes,
		maxMessageBytes:       maxMessageBytes,
		skipOversizedMessages: skipOversizedMessages,
		responseHandler:       responseHandler,
	}
	return batch
}

func (b *Batch) StartBatchTicker() {
	go func() {
		for {
			<-b.batchTicker.C
			b.FlushMessages()
		}
	}()
}

func (b *Batch) Close() {
	b.batchTicker.Stop()
	b.FlushMessages()
}

func (b *Batch) HasPendingMessages() bool {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	return len(b.messages) > 0 || b.hasPendingMessages
}

func (b *Batch) AddEvents(ctx *replication.ListenerContext, messages []gokafka.Message, eventTime time.Time, isLastChunk bool) {
	b.flushLock.Lock()

	for i := range messages {
		msgSize := messageSize(&messages[i])
		// Flush the already-buffered messages before appending one that would
		// push the batch past producerBatchBytes, so a single request never
		// exceeds the configured limit (and the broker's message.max.bytes).
		if len(b.messages) > 0 && b.currentMessageBytes+msgSize > b.batchBytes {
			b.flushMessages()
		}

		b.messages = append(b.messages, messages[i])
		b.currentMessageBytes += msgSize
	}

	b.hasPendingMessages = true
	if isLastChunk {
		b.lastAckCtx = ctx
	}

	shouldFlush := len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.SetProcessLatency(time.Since(eventTime).Nanoseconds())
	}

	if shouldFlush {
		b.FlushMessages()
	}
}

func (b *Batch) FlushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	b.flushMessages()
}

// flushMessages performs the flush. The caller must hold flushLock.
func (b *Batch) flushMessages() {
	if len(b.messages) == 0 {
		return
	}

	messagesToSend := b.messages
	if b.skipOversizedMessages {
		messagesToSend = b.removeOversizedMessages(b.messages)
	}

	startedTime := time.Now()
	var err error
	if len(messagesToSend) > 0 {
		err = b.Writer.WriteMessages(context.Background(), messagesToSend...)
	}

	b.metric.SetBulkRequestProcessLatency(time.Since(startedTime).Nanoseconds())

	flushSuccess := b.handleFlushResult(err, messagesToSend)

	b.messages = b.messages[:0]
	b.currentMessageBytes = 0

	if flushSuccess {
		b.hasPendingMessages = false
		if b.lastAckCtx != nil {
			if ackErr := b.lastAckCtx.Ack(); ackErr != nil {
				logger.Error("ack", "error", ackErr)
			}
			b.lastAckCtx = nil
		}
	} else {
		logger.Warn("flush failed, skipping ACK to preserve message ordering")
	}

	b.batchTicker.Reset(b.batchTickerDuration)
}

func (b *Batch) removeOversizedMessages(messages []gokafka.Message) []gokafka.Message {
	if b.maxMessageBytes <= 0 {
		return messages
	}

	valid := make([]gokafka.Message, 0, len(messages))
	for i := range messages {
		if messageSize(&messages[i]) > b.maxMessageBytes {
			b.notifyOversizedSkipped(&messages[i])
			continue
		}
		valid = append(valid, messages[i])
	}
	return valid
}

func (b *Batch) notifyOversizedSkipped(message *gokafka.Message) {
	if b.responseHandler == nil {
		return
	}

	b.metric.IncrementErrOp(message.Topic)
	b.responseHandler.OnError(&kafka.ResponseHandlerContext{
		Message: message,
		Err:     gokafka.MessageSizeTooLarge,
	})
}

func (b *Batch) handleFlushResult(err error, sent []gokafka.Message) bool {
	if b.responseHandler == nil {
		return err == nil && len(sent) > 0
	}

	switch e := err.(type) { //nolint:errorLint
	case nil:
		if len(sent) > 0 {
			b.handleResponseSuccessFor(sent)
		}
		return len(sent) > 0 || b.skipOversizedMessages
	case gokafka.WriteErrors:
		return b.handleWriteError(e, sent)
	case gokafka.MessageTooLargeError:
		return b.handleMessageTooLargeError(e)
	default:
		b.handleResponseErrorFor(sent, e)
		logger.Error("batch producer flush", "error", err)
		return false
	}
}

func (b *Batch) handleWriteError(writeErrors gokafka.WriteErrors, sent []gokafka.Message) bool {
	hasBlockingError := false
	succeeded := make([]*gokafka.Message, 0, len(sent))

	for i := range writeErrors {
		if writeErrors[i] != nil {
			b.metric.IncrementErrOp(sent[i].Topic)
			b.responseHandler.OnError(&kafka.ResponseHandlerContext{
				Message: &sent[i],
				Err:     writeErrors[i],
			})
			if !b.skipOversizedMessages || !kafka.IsMessageTooLarge(writeErrors[i]) {
				hasBlockingError = true
			}
			continue
		}

		succeeded = append(succeeded, &sent[i])
	}

	b.notifySuccess(succeeded)

	return len(succeeded) > 0 && !hasBlockingError
}

func (b *Batch) handleResponseErrorFor(sent []gokafka.Message, err error) {
	for i := range sent {
		b.metric.IncrementErrOp(sent[i].Topic)
		b.responseHandler.OnError(&kafka.ResponseHandlerContext{
			Message: &sent[i],
			Err:     err,
		})
	}
}

func (b *Batch) handleResponseSuccessFor(sent []gokafka.Message) {
	succeeded := make([]*gokafka.Message, len(sent))
	for i := range sent {
		succeeded[i] = &sent[i]
	}
	b.notifySuccess(succeeded)
}

// notifySuccess dispatches successfully written messages to the response
// handler: once via OnBatchSuccess when the handler implements
// kafka.BatchResponseHandler, otherwise once per message via OnSuccess.
func (b *Batch) notifySuccess(succeeded []*gokafka.Message) {
	for _, m := range succeeded {
		b.metric.IncrementSuccessOp(m.Topic)
	}
	if len(succeeded) == 0 {
		return
	}

	if bh, ok := b.responseHandler.(kafka.BatchResponseHandler); ok {
		bh.OnBatchSuccess(succeeded)
		return
	}

	for _, m := range succeeded {
		b.responseHandler.OnSuccess(&kafka.ResponseHandlerContext{
			Message: m,
			Err:     nil,
		})
	}
}

func (b *Batch) handleMessageTooLargeError(mTooLargeError gokafka.MessageTooLargeError) bool {
	if b.skipOversizedMessages {
		b.notifyOversizedSkipped(&mTooLargeError.Message)

		remaining := mTooLargeError.Remaining
		if b.maxMessageBytes > 0 {
			remaining = b.removeOversizedMessages(remaining)
		}
		if len(remaining) == 0 {
			return true
		}

		err := b.Writer.WriteMessages(context.Background(), remaining...)
		return b.handleFlushResult(err, remaining)
	}

	b.metric.IncrementErrOp(mTooLargeError.Message.Topic)
	b.responseHandler.OnError(&kafka.ResponseHandlerContext{
		Message: &mTooLargeError.Message,
		Err:     mTooLargeError,
	})
	return false
}

func messageSize(m *gokafka.Message) int64 {
	headerSize := 0
	for _, header := range m.Headers {
		headerSize += 2 + len(header.Key)
		headerSize += len(header.Value)
	}
	return int64(14 + (4 + len(m.Key)) + (4 + len(m.Value)) + headerSize)
}
