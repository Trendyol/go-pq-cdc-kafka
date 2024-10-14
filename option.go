package go_pq_cdc_kafka

import (
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
)

type Option func(Connector)

type Options []Option

func (ops Options) Apply(c Connector) {
	for _, op := range ops {
		op(c)
	}
}

func WithResponseHandler(responseHandler kafka.SinkResponseHandler) Option {
	return func(c Connector) {
		c.(*connector).responseHandler = responseHandler
	}
}

/*func WithPrometheusMetrics(collectors []prometheus.Collector) Option {
	return func(c Connector) {
		c.(*connector).metrics = collectors
	}
}

func WithLogger(l logger.Logger) Option {
	return func(c Connector) {
		c.(*connector).cfg.CDC.Logger.Logger = l
	}
}
*/
