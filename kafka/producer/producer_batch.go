package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"

	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	gokafka "github.com/segmentio/kafka-go"
)

type Batch struct {
	sinkResponseHandler kafka.SinkResponseHandler
	batchTicker         *time.Ticker
	Writer              *gokafka.Writer
	metric              *Metric
	messages            []gokafka.Message
	batchTickerDuration time.Duration
	batchLimit          int
	batchBytes          int64
	currentMessageBytes int64
	flushLock           sync.Mutex
}

func newBatch(batchTime time.Duration, writer *gokafka.Writer, batchLimit int, batchBytes int64, sinkResponseHandler kafka.SinkResponseHandler) *Batch {
	batch := &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metric:              &Metric{},
		messages:            make([]gokafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		batchBytes:          batchBytes,
		sinkResponseHandler: sinkResponseHandler,
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
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
	}
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.KafkaConnectorLatency = time.Since(eventTime).Milliseconds()
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

		if err != nil && b.sinkResponseHandler == nil {
			if isFatalError(err) {
				panic(fmt.Errorf("permanent error on Kafka side %v", err))
			}
			logger.Error("batch producer flush", "error", err)
			return
		}

		b.metric.BatchProduceLatency = time.Since(startedTime).Milliseconds()

		if b.sinkResponseHandler != nil {
			switch e := err.(type) {
			case nil:
				b.handleResponseSuccess()
			case gokafka.WriteErrors:
				b.handleWriteError(e)
			case gokafka.MessageTooLargeError:
				b.handleMessageTooLargeError(e)
				return
			default:
				b.handleResponseError(e)
				logger.Error("batch producer flush", "error", err)
				return
			}
		}
		b.messages = b.messages[:0]
		b.currentMessageBytes = 0
		b.batchTicker.Reset(b.batchTickerDuration)
	}
}

func isFatalError(err error) bool {
	var e gokafka.Error
	ok := errors.As(err, &e)
	if ok && errors.Is(err, gokafka.UnknownTopicOrPartition) {
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

func (b *Batch) handleWriteError(writeErrors gokafka.WriteErrors) {
	for i := range writeErrors {
		if writeErrors[i] != nil {
			b.sinkResponseHandler.OnError(&kafka.SinkResponseHandlerContext{
				Message: &b.messages[i],
				Err:     writeErrors[i],
			})
		} else {
			b.sinkResponseHandler.OnSuccess(&kafka.SinkResponseHandlerContext{
				Message: &b.messages[i],
				Err:     nil,
			})
		}
	}
}

func (b *Batch) handleResponseError(err error) {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnError(&kafka.SinkResponseHandlerContext{
			Message: &msg,
			Err:     err,
		})
	}
}

func (b *Batch) handleResponseSuccess() {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnSuccess(&kafka.SinkResponseHandlerContext{
			Message: &msg,
			Err:     nil,
		})
	}
}

func (b *Batch) handleMessageTooLargeError(mTooLargeError gokafka.MessageTooLargeError) {
	b.sinkResponseHandler.OnError(&kafka.SinkResponseHandlerContext{
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
