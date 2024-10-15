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
	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"
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
	cfg             *config.Connector
	cdc             cdc.Connector
	responseHandler kafka.ResponseHandler
	client          kafka.Client
	metrics         []prometheus.Collector
}

func NewConnector(ctx context.Context, config config.Connector, handler Handler, options ...Option) (Connector, error) {
	config.SetDefault()

	kafkaConnector := &connector{
		cfg:     &config,
		handler: handler,
	}

	Options(options).Apply(kafkaConnector)

	pqCDC, err := cdc.NewConnector(ctx, kafkaConnector.cfg.CDC, kafkaConnector.listener)
	if err != nil {
		return nil, err
	}
	kafkaConnector.cdc = pqCDC
	kafkaConnector.cfg.CDC = *pqCDC.GetConfig()

	kafkaClient, err := kafka.NewClient(kafkaConnector.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "kafka new client")
	}
	kafkaConnector.client = kafkaClient

	kafkaConnector.producer, err = producer.NewProducer(kafkaClient, kafkaConnector.cfg, kafkaConnector.responseHandler)
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

	mappedTopicName, ok := c.processMessage(msg)
	if !ok {
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
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
		events[i].Topic = getTopicName(mappedTopicName, events[i].Topic)
	}

	batchSizeLimit := c.cfg.Kafka.ProducerBatchSize
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

func getTopicName(defaultTopic, messageTopic string) string {
	if messageTopic != "" {
		return messageTopic
	}

	return defaultTopic
}

func (c *connector) processMessage(msg *Message) (string, bool) {
	if len(c.cfg.Kafka.TableTopicMapping) == 0 {
		return "", true
	}

	fullTableName := fmt.Sprintf("%s.%s", msg.TableNamespace, msg.TableName)

	if name, exists := c.cfg.Kafka.TableTopicMapping[fullTableName]; exists {
		return name, true
	}

	t, ok := timescaledb.HyperTables.Load(fullTableName)
	if !ok {
		return "", false
	}

	name, exists := c.cfg.Kafka.TableTopicMapping[t.(string)]
	if exists {
		return name, true
	}

	return "", false
}
