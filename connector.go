package cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"

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
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
}

type connector struct {
	partitionCache  sync.Map
	cdc             cdc.Connector
	responseHandler kafka.ResponseHandler
	client          kafka.Client
	producer        producer.Producer
	handler         Handler
	cfg             *config.Connector
	readyCh         chan struct{}
	metrics         []prometheus.Collector
}

func NewConnector(ctx context.Context, config config.Connector, handler Handler, options ...Option) (Connector, error) {
	config.SetDefault()

	kafkaConnector := &connector{
		cfg:     &config,
		handler: handler,
		readyCh: make(chan struct{}, 1),
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

	if kafkaConnector.responseHandler == nil {
		kafkaConnector.responseHandler = &kafka.DefaultResponseHandler{}
	}

	kafkaConnector.producer, err = producer.NewProducer(kafkaClient, kafkaConnector.cfg, kafkaConnector.responseHandler, pqCDC)
	if err != nil {
		logger.Error("kafka new producer", "error", err)
		return nil, err
	}

	pqCDC.SetMetricCollectors(kafkaConnector.producer.GetMetric().PrometheusCollectors()...)
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
		c.readyCh <- struct{}{}
	}()
	c.cdc.Start(ctx)
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

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
	case *format.Snapshot:
		msg = NewSnapshotMessage(m)
	default:
		return
	}

	fullTableName := c.getFullTableName(msg.TableNamespace, msg.TableName)

	topicName, ok := c.resolveTableToTopicName(fullTableName, msg.TableNamespace, msg.TableName)
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
		events[i].Topic = getTopicName(topicName, events[i].Topic)
	}

	batchSizeLimit := c.cfg.Kafka.ProducerBatchSize
	if len(events) > batchSizeLimit {
		chunks := slices.ChunkWithSize(events, batchSizeLimit)
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

func (c *connector) resolveTableToTopicName(fullTableName, tableNamespace, tableName string) (string, bool) {
	tableTopicMapping := c.cfg.Kafka.TableTopicMapping
	if len(tableTopicMapping) == 0 {
		return "", true
	}

	if topicName, exists := tableTopicMapping[fullTableName]; exists {
		return topicName, true
	}

	if t, ok := timescaledb.HyperTables.Load(fullTableName); ok {
		parentName := t.(string)
		if topicName, exists := tableTopicMapping[parentName]; exists {
			return topicName, true
		}
	}

	parentTableName := c.getParentTableName(fullTableName, tableNamespace, tableName)
	if parentTableName != "" {
		if topicName, exists := tableTopicMapping[parentTableName]; exists {
			return topicName, true
		}
	}

	return "", false
}

func (c *connector) getParentTableName(fullTableName, tableNamespace, tableName string) string {
	if cachedValue, found := c.partitionCache.Load(fullTableName); found {
		parentName, ok := cachedValue.(string)
		if !ok {
			logger.Error("invalid cache value type for table", "table", fullTableName)
			return ""
		}

		if parentName != "" {
			logger.Debug("matched partition table to parent from cache",
				"partition", fullTableName,
				"parent", parentName)
		}
		return parentName
	}

	parentTableName := c.findParentTable(tableNamespace, tableName)
	c.partitionCache.Store(fullTableName, parentTableName)

	if parentTableName != "" {
		logger.Debug("matched partition table to parent",
			"partition", fullTableName,
			"parent", parentTableName)
	}

	return parentTableName
}

func (c *connector) findParentTable(tableNamespace, tableName string) string {
	tableParts := strings.Split(tableName, "_")
	if len(tableParts) <= 1 {
		return ""
	}

	for i := 1; i < len(tableParts); i++ {
		parentNameCandidate := strings.Join(tableParts[:i], "_")
		fullParentName := c.getFullTableName(tableNamespace, parentNameCandidate)

		if _, exists := c.cfg.Kafka.TableTopicMapping[fullParentName]; exists {
			return fullParentName
		}
	}

	return ""
}

func (c *connector) getFullTableName(tableNamespace, tableName string) string {
	return fmt.Sprintf("%s.%s", tableNamespace, tableName)
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
