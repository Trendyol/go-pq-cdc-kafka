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
	responseHandler     kafka.ResponseHandler
	batchTicker         *time.Ticker
	Writer              *gokafka.Writer
	metric              Metric
	messages            []gokafka.Message
	batchTickerDuration time.Duration
	batchLimit          int
	batchBytes          int64
	lastAckCtx          *replication.ListenerContext
	currentMessageBytes int64
	flushLock           sync.Mutex
}

func newBatch(
	batchTime time.Duration,
	writer *gokafka.Writer,
	batchLimit int,
	batchBytes int64,
	responseHandler kafka.ResponseHandler,
	slotName string,
	pqCDC cdc.Connector,
) *Batch {
	batch := &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metric:              NewMetric(pqCDC, slotName),
		messages:            make([]gokafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		batchBytes:          batchBytes,
		responseHandler:     responseHandler,
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

func (b *Batch) AddEvents(ctx *replication.ListenerContext, messages []gokafka.Message, eventTime time.Time, isLastChunk bool) {
	b.flushLock.Lock()

	b.messages = append(b.messages, messages...)
	b.currentMessageBytes += totalSizeOfMessages(messages)
	if isLastChunk {
		b.lastAckCtx = ctx
	}
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.SetProcessLatency(time.Since(eventTime).Nanoseconds())
	}

	if len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes {
		b.FlushMessages()
	}
}

func (b *Batch) FlushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.messages) > 0 {
		startedTime := time.Now()
		err := b.Writer.WriteMessages(context.Background(), b.messages...)

		b.metric.SetBulkRequestProcessLatency(time.Since(startedTime).Nanoseconds())

		if b.responseHandler != nil {
			switch e := err.(type) { //nolint:errorLint
			case nil:
				b.handleResponseSuccess()
			case gokafka.WriteErrors:
				b.handleWriteError(e)
			case gokafka.MessageTooLargeError:
				b.handleMessageTooLargeError(e)
			default:
				b.handleResponseError(e)
				logger.Error("batch producer flush", "error", err)
			}
		}
		b.messages = b.messages[:0]
		b.currentMessageBytes = 0
		if b.lastAckCtx != nil {
			if err := b.lastAckCtx.Ack(); err != nil {
				logger.Error("ack", "error", err)
			}
			b.lastAckCtx = nil
		}
		b.batchTicker.Reset(b.batchTickerDuration)
	}
}
func (b *Batch) handleWriteError(writeErrors gokafka.WriteErrors) {
	for i := range writeErrors {
		if writeErrors[i] != nil {
			b.responseHandler.OnError(&kafka.ResponseHandlerContext{
				Message: &b.messages[i],
				Err:     writeErrors[i],
			})
		} else {
			b.responseHandler.OnSuccess(&kafka.ResponseHandlerContext{
				Message: &b.messages[i],
				Err:     nil,
			})
		}
	}
}

func (b *Batch) handleResponseError(err error) {
	for _, msg := range b.messages {
		b.metric.IncrementErrOp(msg.Topic)
		b.responseHandler.OnError(&kafka.ResponseHandlerContext{
			Message: &msg,
			Err:     err,
		})
	}
}

func (b *Batch) handleResponseSuccess() {
	for _, msg := range b.messages {
		b.metric.IncrementSuccessOp(msg.Topic)
		b.responseHandler.OnSuccess(&kafka.ResponseHandlerContext{
			Message: &msg,
			Err:     nil,
		})
	}
}

func (b *Batch) handleMessageTooLargeError(mTooLargeError gokafka.MessageTooLargeError) {
	b.responseHandler.OnError(&kafka.ResponseHandlerContext{
		Message: &mTooLargeError.Message,
		Err:     mTooLargeError,
	})
}

func totalSizeOfMessages(messages []gokafka.Message) int64 {
	var size int
	for _, m := range messages {
		headerSize := 0
		for _, header := range m.Headers {
			headerSize += 2 + len(header.Key)
			headerSize += len(header.Value)
		}
		size += 14 + (4 + len(m.Key)) + (4 + len(m.Value)) + headerSize
	}
	return int64(size)
}
