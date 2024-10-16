package producer

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "go_pq_cdc_kafka"

type Metric interface {
	SetProcessLatency(latency int64)
	SetBulkRequestProcessLatency(latency int64)
	PrometheusCollectors() []prometheus.Collector
}

type metric struct {
	processLatencyMs            prometheus.Gauge
	bulkRequestProcessLatencyMs prometheus.Gauge
}

func NewMetric(slotName string) Metric {
	hostname, _ := os.Hostname()
	return &metric{
		processLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "process_latency",
			Name:      "current",
			Help:      "latest kafka connector process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
		bulkRequestProcessLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "bulk_request_process_latency",
			Name:      "current",
			Help:      "latest kafka connector bulk request process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
	}
}

func (m *metric) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.processLatencyMs,
		m.bulkRequestProcessLatencyMs,
	}
}

func (m *metric) SetProcessLatency(latency int64) {
	m.processLatencyMs.Set(float64(latency))
}

func (m *metric) SetBulkRequestProcessLatency(latency int64) {
	m.bulkRequestProcessLatencyMs.Set(float64(latency))
}
