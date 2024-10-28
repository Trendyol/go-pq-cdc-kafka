package producer

import (
	"os"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "go_pq_cdc_kafka"

type Metric interface {
	SetProcessLatency(latency int64)
	SetBulkRequestProcessLatency(latency int64)
	PrometheusCollectors() []prometheus.Collector
	IncrementSuccessOp(topicName string)
	IncrementErrOp(topicName string)
}

var hostname, _ = os.Hostname()

type metric struct {
	pqCDC                       cdc.Connector
	processLatencyMs            prometheus.Gauge
	bulkRequestProcessLatencyMs prometheus.Gauge
	totalSuccess                map[string]prometheus.Counter
	totalErr                    map[string]prometheus.Counter
	slotName                    string
}

func NewMetric(pqCDC cdc.Connector, slotName string) Metric {
	return &metric{
		pqCDC:    pqCDC,
		slotName: slotName,
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
		totalSuccess: make(map[string]prometheus.Counter),
		totalErr:     make(map[string]prometheus.Counter),
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

func (m *metric) IncrementSuccessOp(topicName string) {
	if _, exists := m.totalSuccess[topicName]; !exists {
		m.totalSuccess[topicName] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "write",
			Name:      "total",
			Help:      "total number of successful in write operation to kafka",
			ConstLabels: prometheus.Labels{
				"slot_name":  m.slotName,
				"topic_name": topicName,
				"host":       hostname,
			},
		})
		m.pqCDC.SetMetricCollectors(m.totalSuccess[topicName])
	}

	m.totalSuccess[topicName].Add(1)
}

func (m *metric) IncrementErrOp(topicName string) {
	if _, exists := m.totalErr[topicName]; !exists {
		m.totalErr[topicName] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "err",
			Name:      "total",
			Help:      "total number of error in write operation to kafka",
			ConstLabels: prometheus.Labels{
				"slot_name":  m.slotName,
				"topic_name": topicName,
				"host":       hostname,
			},
		})
		m.pqCDC.SetMetricCollectors(m.totalErr[topicName])
	}

	m.totalErr[topicName].Add(1)
}
