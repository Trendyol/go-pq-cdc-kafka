package producer

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	cdcKafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	cdcLogger "github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
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

	if ok, _ := batch.handleWriteError(writeErrors, sent); !ok {
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

	if ok, _ := batch.handleWriteError(writeErrors, sent); !ok {
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
func newTestBatch(handler cdcKafka.ResponseHandler) *Batch {
	return &Batch{
		responseHandler:     handler,
		metric:              noopMetric{},
		batchTicker:         time.NewTicker(time.Hour),
		batchTickerDuration: time.Hour,
		batchLimit:          100,
		batchBytes:          1 << 20,
		closing:             make(chan struct{}),
	}
}

func TestFlushMessages_FailedWrite_RetriesUntilSuccessThenAcks(t *testing.T) {
	handler := &recordingHandler{}
	batch := newTestBatch(handler)
	defer batch.batchTicker.Stop()

	var writes [][]gokafka.Message
	batch.writeMessages = func(_ context.Context, msgs ...gokafka.Message) error {
		writes = append(writes, append([]gokafka.Message(nil), msgs...))
		if len(writes) < 3 {
			return errors.New("broker down")
		}
		return nil
	}

	acked := 0
	ctx := &replication.ListenerContext{Ack: func() error { acked++; return nil }}
	batch.AddEvents(ctx, []gokafka.Message{
		{Topic: "test.topic", Value: []byte("a")},
		{Topic: "test.topic", Value: []byte("b")},
	}, time.Now(), true)
	batch.FlushMessages()

	if acked != 1 {
		t.Fatalf("expected exactly one ack after the successful retry, got %d", acked)
	}
	if len(writes) != 3 {
		t.Fatalf("expected 3 write attempts, got %d", len(writes))
	}
	for i, w := range writes {
		if len(w) != 2 {
			t.Fatalf("attempt %d: expected whole batch re-sent in order, got %d messages", i+1, len(w))
		}
	}
	if handler.errors != 4 {
		t.Fatalf("expected OnError per message per failed attempt (4), got %d", handler.errors)
	}
	if batch.HasPendingMessages() {
		t.Fatal("expected no pending messages after successful retry")
	}
}

func TestFlushMessages_SkipOversized_AllOversizedWriteErrorsDoNotRetry(t *testing.T) {
	handler := &recordingHandler{}
	batch := newTestBatch(handler)
	batch.skipOversizedMessages = true
	defer batch.batchTicker.Stop()

	attempts := 0
	batch.writeMessages = func(_ context.Context, msgs ...gokafka.Message) error {
		attempts++
		errs := make(gokafka.WriteErrors, len(msgs))
		for i := range errs {
			errs[i] = gokafka.MessageSizeTooLarge
		}
		return errs
	}

	batch.AddEvents(nil, []gokafka.Message{{Topic: "t", Value: []byte("big")}}, time.Now(), true)
	batch.FlushMessages()

	if attempts != 1 {
		t.Fatalf("expected a single attempt for a batch of only skippable oversized messages, got %d", attempts)
	}
	if batch.HasPendingMessages() {
		t.Fatal("expected batch to be cleared")
	}
}

func TestFlushMessages_SkipOversized_MixedWriteErrorsRetryWithoutOversized(t *testing.T) {
	handler := &recordingHandler{}
	batch := newTestBatch(handler)
	batch.skipOversizedMessages = true
	defer batch.batchTicker.Stop()

	var writes [][]gokafka.Message
	batch.writeMessages = func(_ context.Context, msgs ...gokafka.Message) error {
		writes = append(writes, append([]gokafka.Message(nil), msgs...))
		if len(writes) == 1 {
			return gokafka.WriteErrors{nil, gokafka.MessageSizeTooLarge, errors.New("transient")}
		}
		return nil
	}

	batch.AddEvents(nil, []gokafka.Message{
		{Topic: "t", Value: []byte("ok")},
		{Topic: "t", Value: []byte("big")},
		{Topic: "t", Value: []byte("fail")},
	}, time.Now(), true)
	batch.FlushMessages()

	if len(writes) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(writes))
	}
	if len(writes[1]) != 2 || string(writes[1][0].Value) != "ok" || string(writes[1][1].Value) != "fail" {
		t.Fatalf("expected retry to re-send batch in order without the oversized message, got %v", writes[1])
	}
}

func TestFlushMessages_SkipOversized_MessageTooLargeRetryDropsOversized(t *testing.T) {
	handler := &recordingHandler{}
	batch := newTestBatch(handler)
	batch.skipOversizedMessages = true
	defer batch.batchTicker.Stop()

	big := gokafka.Message{Topic: "t", Value: []byte("big")}
	rest := []gokafka.Message{{Topic: "t", Value: []byte("a")}, {Topic: "t", Value: []byte("b")}}

	var writes [][]gokafka.Message
	batch.writeMessages = func(_ context.Context, msgs ...gokafka.Message) error {
		writes = append(writes, append([]gokafka.Message(nil), msgs...))
		switch len(writes) {
		case 1:
			return gokafka.MessageTooLargeError{Message: big, Remaining: rest}
		case 2:
			return errors.New("transient")
		default:
			return nil
		}
	}

	batch.AddEvents(nil, append([]gokafka.Message{big}, rest...), time.Now(), true)
	batch.FlushMessages()

	if len(writes) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(writes))
	}
	if len(writes[2]) != 2 {
		t.Fatalf("expected the oversized message not to be re-sent, got %d messages", len(writes[2]))
	}
	if handler.errors != 3 {
		t.Fatalf("expected 1 oversized + 2 transient OnError calls, got %d", handler.errors)
	}
}

func TestFlushMessages_CloseDuringOutage_StopsRetryWithoutAck(t *testing.T) {
	handler := &recordingHandler{}
	batch := newTestBatch(handler)
	batch.Writer = &gokafka.Writer{}
	batch.writeMessages = func(_ context.Context, _ ...gokafka.Message) error { return errors.New("broker down") }

	acked := 0
	ctx := &replication.ListenerContext{Ack: func() error { acked++; return nil }}
	batch.AddEvents(ctx, []gokafka.Message{{Topic: "t", Value: []byte("a")}}, time.Now(), true)

	done := make(chan struct{})
	go func() { batch.FlushMessages(); close(done) }()
	time.Sleep(300 * time.Millisecond)

	closed := make(chan struct{})
	go func() { batch.Close(); close(closed) }()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close must return while the broker is down")
	}
	<-done
	if acked != 0 {
		t.Fatalf("unwritten messages must not be acked, got %d acks", acked)
	}
}
