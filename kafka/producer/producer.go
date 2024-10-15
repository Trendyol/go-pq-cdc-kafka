package producer

import (
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/internal/bytes"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/pkg/errors"
	gokafka "github.com/segmentio/kafka-go"
)

type Metric struct {
	KafkaConnectorLatency int64
	BatchProduceLatency   int64
}

type Producer struct {
	ProducerBatch *Batch
}

func NewProducer(
	kafkaClient kafka.Client,
	config *config.Connector,
	responseHandler kafka.ResponseHandler,
) (Producer, error) {
	writer := kafkaClient.Producer()

	batchBytes, err := bytes.ParseSize(config.Kafka.ProducerBatchBytes)
	if err != nil {
		return Producer{}, errors.Wrap(err, "producerBatchBytes parse")
	}

	return Producer{
		ProducerBatch: newBatch(
			config.Kafka.ProducerBatchTickerDuration,
			writer,
			config.Kafka.ProducerBatchSize,
			int64(batchBytes),
			responseHandler,
		),
	}, nil
}

func (p *Producer) StartBatch() {
	p.ProducerBatch.StartBatchTicker()
}

func (p *Producer) Produce(
	ctx *replication.ListenerContext,
	eventTime time.Time,
	messages []gokafka.Message,
	isLastChunk bool,
) {
	p.ProducerBatch.AddEvents(ctx, messages, eventTime, isLastChunk)
}

func (p *Producer) Close() error {
	p.ProducerBatch.Close()
	return p.ProducerBatch.Writer.Close()
}

func (p *Producer) GetMetric() *Metric {
	return p.ProducerBatch.metric
}
