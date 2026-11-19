package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	corecdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/prometheus/client_golang/prometheus"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestExistingConstructorOnlineUsesHandlerAndAcksAllDefault(t *testing.T) {
	cfg := validOnlineConfig()
	cfg.Kafka.RequiredAcks = 0
	calls := 0
	handler := func(message *Message) []gokafka.Message {
		calls++
		require.Equal(t, InsertMessage, message.Type)
		return []gokafka.Message{{Key: []byte("custom-key"), Value: []byte("transformed"), Topic: "custom.topic"}}
	}
	connector, err := NewConnector(t.Context(), cfg, handler)
	require.NoError(t, err, "existing NewConnector must select online internally")
	t.Cleanup(connector.Close)
	online, ok := connector.(*onlineConnector)
	require.True(t, ok)
	require.Equal(t, -1, online.cfg.Kafka.RequiredAcks)
	writer := newControlledWriter()
	online.delivery.writer = writer
	require.NoError(t, online.delivery.Handle(t.Context(), onlineEnvelopeEvent("INSERT", 1, map[string]any{"id": int32(1)})))
	require.Equal(t, 1, calls)
	records := writer.writtenMessages()
	require.Len(t, records, 1)
	require.Equal(t, "custom.topic", records[0].Topic)
	require.Equal(t, []byte("custom-key"), records[0].Key)
	require.Equal(t, []byte("transformed"), records[0].Value)
}

func TestExistingConstructorHandlerReceivesOwnedBeforeAfterAndSourceMetadata(t *testing.T) {
	clock := time.Date(2026, 11, 13, 9, 10, 11, 0, time.UTC)
	for _, operation := range []corecdc.OnlineOperation{corecdc.OnlineRead, corecdc.OnlineInsert, corecdc.OnlineUpdate, corecdc.OnlineDelete} {
		t.Run(string(operation), func(t *testing.T) {
			image := map[string]any{"id": int32(7), "small": int16(3), "big": int64(9007199254740993), "json": map[string]any{"nested": []any{json.Number("9007199254740993")}}}
			before := map[string]any{"id": int32(7), "json": map[string]any{"nested": []any{"old"}}}
			event := onlineEnvelopeEvent(operation, 7, image)
			event.LSN, event.TransactionID, event.EventTime = 50, 99, clock
			if operation == corecdc.OnlineUpdate || operation == corecdc.OnlineDelete {
				event.Before = before
			}
			if operation == corecdc.OnlineDelete {
				event.Image = nil
			}
			if operation == corecdc.OnlineRead {
				event.LSN, event.TransactionID, event.EventTime = 0, 0, time.Time{}
			}
			calls := 0
			connector, err := NewConnector(t.Context(), validOnlineConfig(), func(message *Message) []gokafka.Message {
				calls++
				expectedType := MessageType(operation)
				if operation == corecdc.OnlineRead {
					expectedType = SnapshotMessage
				}
				require.Equal(t, expectedType, message.Type)
				require.Equal(t, "public", message.TableNamespace)
				require.Equal(t, "accounts", message.TableName)
				require.Equal(t, event.Image, message.NewData)
				require.Equal(t, event.Before, message.OldData)
				if operation == corecdc.OnlineRead {
					require.Equal(t, event.Order.EndLSN, message.LSN)
					require.True(t, message.EventTime.IsZero())
					require.Zero(t, message.TransactionID)
				} else {
					require.Equal(t, pq.LSN(50), message.LSN, "actual WALStart, never CommitLSN")
					require.Equal(t, uint32(99), message.TransactionID)
					require.Equal(t, clock, message.EventTime)
				}
				if message.NewData != nil {
					message.NewData["json"].(map[string]any)["nested"].([]any)[0] = "mutated"
				}
				if message.OldData != nil {
					message.OldData["json"].(map[string]any)["nested"].([]any)[0] = "mutated"
				}
				message.LSN = 1
				message.TableName = "mutated"
				return []gokafka.Message{{Key: []byte("key"), Value: []byte("payload")}}
			})
			require.NoError(t, err)
			t.Cleanup(connector.Close)
			online := connector.(*onlineConnector)
			writer := newControlledWriter()
			online.delivery.writer = writer
			require.NoError(t, online.delivery.Handle(t.Context(), event))
			require.Equal(t, 1, calls)
			require.Equal(t, json.Number("9007199254740993"), image["json"].(map[string]any)["nested"].([]any)[0])
			require.Equal(t, "old", before["json"].(map[string]any)["nested"].([]any)[0])
		})
	}
}

func TestExistingConstructorHandlerFanoutPreservesPayloadAndStableMetadata(t *testing.T) {
	value := []byte("caller-value")
	key := []byte("caller-key")
	header := []byte("caller-header")
	returned := []gokafka.Message{
		{Key: key, Value: value, Headers: []gokafka.Header{{Key: "custom-header", Value: header}}},
		{Topic: "alternate.topic", Key: []byte("other-key"), Value: []byte("other-value")},
	}
	connector, err := NewConnector(t.Context(), validOnlineConfig(), func(*Message) []gokafka.Message { return returned })
	require.NoError(t, err)
	t.Cleanup(connector.Close)
	online := connector.(*onlineConnector)
	writer := newControlledWriter()
	online.delivery.writer = writer
	event := onlineEnvelopeEvent(corecdc.OnlineInsert, 7, map[string]any{"id": int32(7)})
	require.NoError(t, online.delivery.Handle(t.Context(), event), "all handler outputs must be durable")
	records := writer.writtenMessages()
	require.Len(t, records, 2)
	require.Empty(t, returned[0].Topic, "adapter must not mutate caller records")
	require.Len(t, returned[0].Headers, 1)
	require.Equal(t, "accounts.topic", records[0].Topic)
	require.Equal(t, "alternate.topic", records[1].Topic)
	for i, record := range records {
		headers := map[string]string{}
		for _, h := range record.Headers {
			headers[h.Key] = string(h.Value)
		}
		require.Equal(t, event.ID, headers["cdc-event-id"])
		require.Equal(t, "1", headers["cdc-metadata-version"])
		require.Equal(t, event.Order.EndLSN.String(), headers["cdc-order-end-lsn"])
		require.Equal(t, strconv.Itoa(int(event.Order.Phase)), headers["cdc-order-phase"])
		require.Equal(t, strconv.Itoa(int(event.Order.Ordinal)), headers["cdc-order-ordinal"])
		require.Equal(t, strconv.Itoa(i), headers["cdc-output-index"])
		require.NotEmpty(t, headers["cdc-output-id"])
	}
	require.NotEqual(t, records[0].Headers[len(records[0].Headers)-1].Value, records[1].Headers[len(records[1].Headers)-1].Value)
	value[0], key[0], header[0] = 'X', 'X', 'X'
	returned[0].Headers[0].Key = "mutated"
	require.Equal(t, []byte("caller-value"), records[0].Value)
	require.Equal(t, []byte("caller-key"), records[0].Key)
	require.Equal(t, "custom-header", records[0].Headers[0].Key)
	require.Equal(t, []byte("caller-header"), records[0].Headers[0].Value)
	firstHeaders := records[1].Headers
	require.NoError(t, online.delivery.Handle(t.Context(), event))
	require.Equal(t, firstHeaders, writer.writtenMessages()[3].Headers, "same event/output ordinal has stable replay identity")
}

func TestExistingConstructorHandlerRejectsUnsafeFanoutBeforeAnyWrite(t *testing.T) {
	valid := func() gokafka.Message { return gokafka.Message{Key: []byte("key"), Value: []byte("value")} }
	cases := []struct {
		name      string
		records   []gokafka.Message
		configure func(*config.Connector)
	}{
		{name: "empty filter", records: nil},
		{name: "missing key", records: []gokafka.Message{{Value: []byte("value")}}},
		{name: "empty value", records: []gokafka.Message{{Key: []byte("key"), Value: []byte{}}}},
		{name: "raw tombstone", records: []gokafka.Message{{Key: []byte("key")}}},
		{name: "invalid later topic", records: []gokafka.Message{valid(), {Key: []byte("key"), Value: []byte("value"), Topic: "invalid/topic"}}},
		{name: "reserved later header", records: []gokafka.Message{valid(), {Key: []byte("key"), Value: []byte("value"), Headers: []gokafka.Header{{Key: "cdc-event-id", Value: []byte("forged")}}}}},
		{name: "empty header key", records: []gokafka.Message{{Key: []byte("key"), Value: []byte("value"), Headers: []gokafka.Header{{}}}}},
		{name: "oversize later value", records: []gokafka.Message{valid(), {Key: []byte("key"), Value: make([]byte, 2048)}}, configure: func(cfg *config.Connector) { cfg.Kafka.MaxMessageBytes = "1kb" }},
		{name: "fanout count", records: []gokafka.Message{valid(), valid()}, configure: func(cfg *config.Connector) { cfg.Kafka.ProducerBatchSize = 1 }},
		{name: "fanout bytes", records: []gokafka.Message{valid(), valid()}, configure: func(cfg *config.Connector) { cfg.Kafka.ProducerBatchBytes = "512b" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := validOnlineConfig()
			if test.configure != nil {
				test.configure(&cfg)
			}
			connector, err := NewConnector(t.Context(), cfg, func(*Message) []gokafka.Message { return test.records })
			require.NoError(t, err)
			t.Cleanup(connector.Close)
			online := connector.(*onlineConnector)
			writer := newControlledWriter()
			online.delivery.writer = writer
			err = online.delivery.Handle(t.Context(), onlineEnvelopeEvent(corecdc.OnlineInsert, 7, map[string]any{"id": int32(7)}))
			require.Error(t, err)
			require.Empty(t, writer.writtenMessages(), "later invalid output cannot produce an earlier partial append")
			require.Equal(t, uint64(1), online.Stats().Delivery.Rejected)
		})
	}
}

// fanoutWriter releases each synchronous single-record write independently.
// This is an I/O double, not a substitute for the genuine broker integration.
type fanoutWriter struct {
	calls   chan gokafka.Message
	results chan error
}

func (w *fanoutWriter) WriteMessages(ctx context.Context, records ...gokafka.Message) error {
	if len(records) != 1 {
		return errors.New("each RequireAll boundary must write exactly one record")
	}
	w.calls <- records[0]
	select {
	case err := <-w.results:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (w *fanoutWriter) Close() error { return nil }

func TestExistingConstructorHandlerCompletesOnlyAfterEveryOutputAndStopsOnPartialFailure(t *testing.T) {
	for _, failure := range []error{nil, gokafka.WriteErrors{gokafka.NotEnoughReplicas}, context.DeadlineExceeded} {
		name := "success"
		if failure != nil {
			name = failure.Error()
		}
		t.Run(name, func(t *testing.T) {
			connector, err := NewConnector(t.Context(), validOnlineConfig(), func(*Message) []gokafka.Message {
				return []gokafka.Message{{Key: []byte("a"), Value: []byte("first")}, {Key: []byte("b"), Value: []byte("second")}, {Key: []byte("c"), Value: []byte("third")}}
			})
			require.NoError(t, err)
			t.Cleanup(connector.Close)
			online := connector.(*onlineConnector)
			writer := &fanoutWriter{calls: make(chan gokafka.Message, 3), results: make(chan error)}
			online.delivery.writer = writer
			result := make(chan error, 1)
			go func() {
				result <- online.delivery.Handle(t.Context(), onlineEnvelopeEvent(corecdc.OnlineInsert, 7, map[string]any{"id": int32(7)}))
			}()
			require.Equal(t, []byte("first"), (<-writer.calls).Value)
			select {
			case err := <-result:
				t.Fatalf("completed before first durable write: %v", err)
			default:
			}
			writer.results <- nil
			require.Equal(t, []byte("second"), (<-writer.calls).Value)
			select {
			case err := <-result:
				t.Fatalf("completed before second durable write: %v", err)
			default:
			}
			writer.results <- failure
			if failure == nil {
				require.Equal(t, []byte("third"), (<-writer.calls).Value)
				select {
				case err := <-result:
					t.Fatalf("completed before final durable write: %v", err)
				default:
				}
				writer.results <- nil
				require.NoError(t, <-result)
				require.Equal(t, uint64(3), online.Stats().Delivery.Written)
			} else {
				err := <-result
				if errors.Is(failure, context.DeadlineExceeded) {
					require.ErrorIs(t, err, ErrOnlineWriteUnconfirmed)
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, err, gokafka.NotEnoughReplicas)
				}
				require.Empty(t, writer.calls, "third output must not be attempted after second failure")
				require.Equal(t, uint64(1), online.Stats().Delivery.Written, "only first result is durable; event is not complete")
			}
		})
	}
}

func TestExistingConstructorOnlineOptionsRemainSafeAndResponseHandlerIsNotReceipt(t *testing.T) {
	handler := func(*Message) []gokafka.Message { return []gokafka.Message{{Key: []byte("k"), Value: []byte("v")}} }
	metric := prometheus.NewCounter(prometheus.CounterOpts{Name: "existing_constructor_online_test_total", Help: "Constructor option forwarding"})
	safeLogger := logger.NewSlog(slog.LevelError)
	connector, err := NewConnector(t.Context(), validOnlineConfig(), handler, nil, WithLogger(safeLogger), WithPrometheusMetrics([]prometheus.Collector{metric}))
	require.NoError(t, err)
	t.Cleanup(connector.Close)
	require.Same(t, safeLogger, connector.(*onlineConnector).cfg.CDC.Logger.Logger)
	_, err = NewConnector(t.Context(), validOnlineConfig(), handler, WithResponseHandler(nil))
	require.ErrorIs(t, err, ErrOnlineAdmission, "even nil response option must not silently change meaning online")
	_, err = NewConnector(t.Context(), validOnlineConfig(), nil)
	require.ErrorIs(t, err, ErrOnlineAdmission)
	for _, bad := range []func(*config.Connector){func(cfg *config.Connector) { cfg.Kafka.ProducerBatchSize = -1 }, func(cfg *config.Connector) { cfg.Kafka.ProducerBatchBytes = "0b" }, func(cfg *config.Connector) { cfg.Kafka.RequiredAcks = 1 }} {
		cfg := validOnlineConfig()
		bad(&cfg)
		_, err = NewConnector(t.Context(), cfg, handler)
		require.ErrorIs(t, err, ErrOnlineAdmission)
	}
}
