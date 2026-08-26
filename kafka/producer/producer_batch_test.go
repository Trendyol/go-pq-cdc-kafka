package producer

import (
	"log/slog"
	"os"
	"testing"

	cdcLogger "github.com/Trendyol/go-pq-cdc/logger"
	cdcKafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMain(m *testing.M) {
	cdcLogger.InitLogger(slog.Default())
	os.Exit(m.Run())
}

type noopMetric struct{}

func (noopMetric) SetProcessLatency(int64)                       {}
func (noopMetric) SetBulkRequestProcessLatency(int64)            {}
func (noopMetric) PrometheusCollectors() []prometheus.Collector { return nil }
func (noopMetric) IncrementSuccessOp(string)                     {}
func (noopMetric) IncrementErrOp(string)                         {}

type recordingHandler struct {
	errors int
}

func (h *recordingHandler) OnSuccess(_ *cdcKafka.ResponseHandlerContext) {}
func (h *recordingHandler) OnError(_ *cdcKafka.ResponseHandlerContext)  { h.errors++ }

func TestRemoveOversizedMessages_SkipsOnlyLargeMessages(t *testing.T) {
	handler := &recordingHandler{}
	batch := &Batch{
		skipOversizedMessages: true,
		maxMessageBytes:       200,
		responseHandler:       handler,
		metric:                noopMetric{},
	}

	messages := []gokafka.Message{
		{Topic: "test.topic", Value: make([]byte, 50)},
		{Topic: "test.topic", Value: make([]byte, 250)},
		{Topic: "test.topic", Value: make([]byte, 80)},
	}

	valid := batch.removeOversizedMessages(messages)

	if len(valid) != 2 {
		t.Fatalf("expected 2 valid messages, got %d", len(valid))
	}
	if handler.errors != 1 {
		t.Fatalf("expected 1 skipped oversized message, got %d", handler.errors)
	}
}

func TestHandleWriteError_SkipOversized_ReturnsTrueWhenOnlySizeErrors(t *testing.T) {
	handler := cdcKafka.NewDefaultResponseHandler(true)
	batch := &Batch{
		skipOversizedMessages: true,
		responseHandler:       handler,
		metric:                noopMetric{},
	}

	sent := []gokafka.Message{
		{Topic: "test.topic", Value: []byte("ok")},
		{Topic: "test.topic", Value: []byte("bad")},
	}
	writeErrors := gokafka.WriteErrors{
		nil,
		gokafka.MessageSizeTooLarge,
	}

	if !batch.handleWriteError(writeErrors, sent) {
		t.Fatal("expected flush success when only non-blocking oversized errors remain")
	}
}

func TestHandleWriteError_SkipDisabled_PanicsOnPartialError(t *testing.T) {
	handler := cdcKafka.NewDefaultResponseHandler(false)
	batch := &Batch{
		skipOversizedMessages: false,
		responseHandler:       handler,
		metric:                noopMetric{},
	}

	sent := []gokafka.Message{
		{Topic: "test.topic", Value: []byte("ok")},
		{Topic: "test.topic", Value: []byte("bad")},
	}
	writeErrors := gokafka.WriteErrors{
		nil,
		gokafka.MessageSizeTooLarge,
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic when skip is disabled")
		}
	}()

	batch.handleWriteError(writeErrors, sent)
}

var _ Metric = noopMetric{}
