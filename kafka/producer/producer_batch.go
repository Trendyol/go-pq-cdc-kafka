package producer

import (
	gKafka "github.com/Trendyol/go-dcp-cdc-kafka/kafka"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

type Batch struct {
	sinkResponseHandler gKafka.SinkResponseHandler
	batchTicker         *time.Ticker
	Writer              *kafka.Writer
	dcpCheckpointCommit func()
	metric              *Metric
	messages            []kafka.Message
	currentMessageBytes int64
	batchTickerDuration time.Duration
	batchLimit          int
	batchBytes          int64
	flushLock           sync.Mutex
	isDcpRebalancing    bool
}

func newBatch(batchTime time.Duration, writer *kafka.Writer, batchLimit int, batchBytes int64, dcpCheckpointCommit func(), sinkResponseHandler gKafka.SinkResponseHandler) *Batch {
	batch := &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metric:              &Metric{},
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		dcpCheckpointCommit: dcpCheckpointCommit,
		batchBytes:          batchBytes,
		sinkResponseHandler: sinkResponseHandler,
	}
	return batch
}
