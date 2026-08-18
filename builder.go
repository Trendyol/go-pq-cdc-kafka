package cdc

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/prometheus/client_golang/prometheus"
)

type ConnectorBuilder struct {
	responseHandler kafka.ResponseHandler
	log             logger.Logger
	handler         Handler
	configPath      string
	metrics         []prometheus.Collector
}

func NewConnectorBuilder(configPath string) *ConnectorBuilder {
	return &ConnectorBuilder{configPath: configPath}
}

func (b *ConnectorBuilder) SetHandler(h Handler) *ConnectorBuilder {
	b.handler = h
	return b
}

func (b *ConnectorBuilder) SetResponseHandler(h kafka.ResponseHandler) *ConnectorBuilder {
	b.responseHandler = h
	return b
}

func (b *ConnectorBuilder) SetLogger(l logger.Logger) *ConnectorBuilder {
	b.log = l
	return b
}

func (b *ConnectorBuilder) SetPrometheusMetrics(collectors []prometheus.Collector) *ConnectorBuilder {
	b.metrics = collectors
	return b
}

func (b *ConnectorBuilder) Build(ctx context.Context) (Connector, error) {
	cfg, err := config.Load(b.configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	opts := make([]Option, 0, 3)
	if b.responseHandler != nil {
		opts = append(opts, WithResponseHandler(b.responseHandler))
	}
	if b.log != nil {
		opts = append(opts, WithLogger(b.log))
	}
	if len(b.metrics) > 0 {
		opts = append(opts, WithPrometheusMetrics(b.metrics))
	}

	handler := b.handler
	if handler == nil {
		handler = defaultHandler(cfg.Mapper)
	}

	return NewConnector(ctx, *cfg, handler, opts...)
}
