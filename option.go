package cdc

import (
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type Option func(Connector)

type Options []Option

func (ops Options) Apply(c Connector) {
	for _, op := range ops {
		op(c)
	}
}

func WithResponseHandler(respHandler kafka.ResponseHandler) Option {
	return func(c Connector) {
		c.(*connector).responseHandler = respHandler
	}
}

func WithPrometheusMetrics(collectors []prometheus.Collector) Option {
	return func(c Connector) {
		c.(*connector).metrics = collectors
	}
}

func WithLogger(l logger.Logger) Option {
	return func(c Connector) {
		c.(*connector).cfg.CDC.Logger.Logger = l
	}
}
