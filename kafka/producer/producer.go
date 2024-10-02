package producer

import (
	"github.com/Trendyol/go-dcp-cdc-kafka/config"
	"github.com/Trendyol/go-dcp-cdc-kafka/kafka"
	"github.com/Trendyol/go-dcp/helpers"
)

type Metric struct {
	KafkaConnectorLatency int64
	BatchProduceLatency   int64
}

type Producer struct {
	ProducerBatch *Batch
}

func NewProducer(kafkaClient kafka.Client,
	config *config.Connector,
	dcpCheckpointCommit func(),
	sinkResponseHandler kafka.SinkResponseHandler,
) (Producer, error) {
	writer := kafkaClient.Producer()

	return Producer{
		ProducerBatch: newBatch(
			config.Kafka.ProducerBatchTickerDuration,
			writer,
			config.Kafka.ProducerBatchSize,
			int64(helpers.ResolveUnionIntOrStringValue(config.Kafka.ProducerBatchBytes)),
			dcpCheckpointCommit,
			sinkResponseHandler,
		),
	}, nil
}
