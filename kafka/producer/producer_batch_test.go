package producer

import (
	"log/slog"
	"os"
	"testing"

	cdcKafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	cdcLogger "github.com/Trendyol/go-pq-cdc/logger"
	"github.com/prometheus/client_golang/prometheus"
	gokafka "github.com/segmentio/kafka-go"
)

func TestMain(m *testing.M) {
	cdcLogger.InitLogger(slog.Default())
	os.Exit(m.Run())
}

type noopMetric struct{}

func (noopMetric) SetProcessLatency(int64)                      {}
func (noopMetric) SetBulkRequestProcessLatency(int64)           {}
func (noopMetric) PrometheusCollectors() []prometheus.Collector { return nil }
func (noopMetric) IncrementSuccessOp(string)                    {}
func (noopMetric) IncrementErrOp(string)                        {}

type recordingHandler struct {
	errors int
}

func (h *recordingHandler) OnSuccess(_ *cdcKafka.ResponseHandlerContext) {}
func (h *recordingHandler) OnError(_ *cdcKafka.ResponseHandlerContext)   { h.errors++ }

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

type batchRecordingHandler struct {
	recordingHandler
	batches   [][]*gokafka.Message
	successes int
}

func (h *batchRecordingHandler) OnSuccess(_ *cdcKafka.ResponseHandlerContext) { h.successes++ }
func (h *batchRecordingHandler) OnBatchSuccess(m []*gokafka.Message) {
	h.batches = append(h.batches, m)
}

func TestNotifySuccess_BatchHandler_CalledOncePerFlushInsteadOfOnSuccess(t *testing.T) {
	handler := &batchRecordingHandler{}
	batch := &Batch{responseHandler: handler, metric: noopMetric{}}

	sent := []gokafka.Message{
		{Topic: "test.topic", Value: []byte("a")},
		{Topic: "test.topic", Value: []byte("b")},
		{Topic: "test.topic", Value: []byte("c")},
	}

	batch.handleResponseSuccessFor(sent)

	if handler.successes != 0 {
		t.Fatalf("expected OnSuccess not to be called, got %d", handler.successes)
	}
	if len(handler.batches) != 1 || len(handler.batches[0]) != 3 {
		t.Fatalf("expected one batch of 3 messages, got %v", handler.batches)
	}
	for i := range sent {
		if handler.batches[0][i] != &sent[i] {
			t.Fatalf("expected message %d in producer order", i)
		}
	}
}

func TestHandleWriteError_BatchHandler_ReceivesOnlySucceededMessages(t *testing.T) {
	handler := &batchRecordingHandler{}
	batch := &Batch{skipOversizedMessages: true, responseHandler: handler, metric: noopMetric{}}

	sent := []gokafka.Message{
		{Topic: "test.topic", Value: []byte("ok-1")},
		{Topic: "test.topic", Value: []byte("bad")},
		{Topic: "test.topic", Value: []byte("ok-2")},
	}
	writeErrors := gokafka.WriteErrors{nil, gokafka.MessageSizeTooLarge, nil}

	if !batch.handleWriteError(writeErrors, sent) {
		t.Fatal("expected flush success when only oversized errors remain")
	}
	if handler.errors != 1 {
		t.Fatalf("expected 1 OnError, got %d", handler.errors)
	}
	if len(handler.batches) != 1 || len(handler.batches[0]) != 2 {
		t.Fatalf("expected one batch of 2 succeeded messages, got %v", handler.batches)
	}
	if string(handler.batches[0][0].Value) != "ok-1" || string(handler.batches[0][1].Value) != "ok-2" {
		t.Fatal("expected only succeeded messages in producer order")
	}
}

func TestNotifySuccess_PlainHandler_KeepsPerMessageOnSuccess(t *testing.T) {
	handler := &countingHandler{}
	batch := &Batch{responseHandler: handler, metric: noopMetric{}}

	batch.handleResponseSuccessFor([]gokafka.Message{{Topic: "t"}, {Topic: "t"}})

	if handler.successes != 2 {
		t.Fatalf("expected 2 OnSuccess calls, got %d", handler.successes)
	}
}

type countingHandler struct{ successes int }

func (h *countingHandler) OnSuccess(_ *cdcKafka.ResponseHandlerContext) { h.successes++ }
func (h *countingHandler) OnError(_ *cdcKafka.ResponseHandlerContext)   {}
