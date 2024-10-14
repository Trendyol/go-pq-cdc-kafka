package cdc

import (
	"context"
	"fmt"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/internal/slices"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka/producer"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	gokafka "github.com/segmentio/kafka-go"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	producer        producer.Producer
	handler         Handler
	config          *config.Connector
	cdc             cdc.Connector
	responseHandler kafka.SinkResponseHandler
	client          kafka.Client
	metrics         []prometheus.Collector
}

func NewConnector(ctx context.Context, config config.Connector, handler Handler, options ...Option) (Connector, error) {
	config.SetDefault()

	kafkaConnector := &connector{
		config:  &config,
		handler: handler,
	}

	Options(options).Apply(kafkaConnector)

	pqCDC, err := cdc.NewConnector(ctx, kafkaConnector.config.CDC, kafkaConnector.listener)
	if err != nil {
		return nil, err
	}
	kafkaConnector.cdc = pqCDC
	kafkaConnector.config.CDC = *pqCDC.GetConfig()

	kafkaClient, err := kafka.NewClient(kafkaConnector.config)
	if err != nil {
		return nil, errors.Wrap(err, "elasticsearch new client")
	}
	kafkaConnector.client = kafkaClient

	kafkaConnector.producer, err = producer.NewProducer(kafkaClient, kafkaConnector.config, kafkaConnector.responseHandler)
	if err != nil {
		logger.Error("kafka new producer", "error", err)
		return nil, err
	}

	pqCDC.SetMetricCollectors(kafkaConnector.metrics...)

	return kafkaConnector, nil
}

func (c *connector) Start(ctx context.Context) {
	go func() {
		logger.Info("waiting for connector start...")
		if err := c.cdc.WaitUntilReady(ctx); err != nil {
			panic(err)
		}
		logger.Info("bulk process started")
		c.producer.StartBatch()
	}()
	c.cdc.Start(ctx)
}

func (c *connector) Close() {
	c.cdc.Close()
	if err := c.producer.Close(); err != nil {
		logger.Error("kafka producer close", "error", err)
	}
}

func (c *connector) listener(ctx *replication.ListenerContext) {
	var msg *Message
	switch m := ctx.Message.(type) {
	case *format.Insert:
		msg = NewInsertMessage(m)
	case *format.Update:
		msg = NewUpdateMessage(m)
	case *format.Delete:
		msg = NewDeleteMessage(m)
	default:
		return
	}

	events := c.handler(msg)
	if len(events) == 0 {
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}

	for i := range events {
		events[i].Topic = getTopicName(c.config, msg.TableNamespace, msg.TableName, events[i].Topic)
	}

	batchSizeLimit := c.config.Kafka.ProducerBatchSize
	if len(events) > batchSizeLimit {
		chunks := slices.ChunkWithSize[gokafka.Message](events, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.producer.Produce(ctx, msg.EventTime, chunk, idx == lastChunkIndex)
		}
	} else {
		c.producer.Produce(ctx, msg.EventTime, events, true)
	}
}

func getTopicName(config *config.Connector, tableNamespace, tableName, actionIndexName string) string {
	if actionIndexName != "" {
		return actionIndexName
	}

	indexName := config.Kafka.CollectionTopicMapping[fmt.Sprintf("%s.%s", tableNamespace, tableName)]
	if indexName == "" {
		panic(fmt.Sprintf("there is no index mapping for table: %s.%s on your configuration", tableNamespace, tableName))
	}

	return indexName
}
