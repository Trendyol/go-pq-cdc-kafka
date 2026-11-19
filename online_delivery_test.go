package cdc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"

	"github.com/stretchr/testify/require"

	gokafka "github.com/segmentio/kafka-go"
)

// controlledWriter is a private test double: it proves the handler only reports
// success after the write call returned nil, and it can hold a write in flight
// to model a broker that has not acknowledged yet.
type controlledWriter struct {
	release  chan struct{}
	started  chan struct{}
	err      error
	block    bool
	mu       sync.Mutex
	written  []gokafka.Message
	closeErr error
	closes   int
}

func newControlledWriter() *controlledWriter {
	return &controlledWriter{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (w *controlledWriter) WriteMessages(ctx context.Context, messages ...gokafka.Message) error {
	select {
	case w.started <- struct{}{}:
	default:
	}
	if w.block {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.mu.Lock()
	w.written = append(w.written, messages...)
	w.mu.Unlock()
	return w.err
}

func (w *controlledWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closes++
	return w.closeErr
}

func (w *controlledWriter) writtenMessages() []gokafka.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]gokafka.Message(nil), w.written...)
}

func newTestDelivery(writer onlineMessageWriter, topics map[string]string, maxBytes int64) *onlineDelivery {
	return &onlineDelivery{writer: writer, topics: topics, maxBytes: maxBytes, counters: &onlineDeliveryCounters{}}
}

func TestOnlineDeliveryReturnsOnlyAfterBrokerAcknowledgement(t *testing.T) {
	writer := newControlledWriter()
	writer.block = true
	delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)
	event := onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1})

	results := make(chan error, 1)
	go func() { results <- delivery.Handle(t.Context(), event) }()

	// The write is in flight and unacknowledged: the handler must not complete.
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached the writer")
	}
	select {
	case err := <-results:
		t.Fatalf("handler returned %v before the broker acknowledged", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, uint64(0), delivery.snapshot().Written)

	close(writer.release)
	require.NoError(t, <-results)
	require.Equal(t, uint64(1), delivery.snapshot().Written)
	require.Equal(t, uint64(0), delivery.snapshot().Failed)

	written := writer.writtenMessages()
	require.Len(t, written, 1)
	require.Equal(t, "accounts.topic", written[0].Topic, "the mapped destination is used, never an empty topic")
	require.Equal(t, OnlineMessageKey("public", "accounts", 1), written[0].Key)
	decoded, err := DecodeOnlineEnvelope(written[0].Value)
	require.NoError(t, err)
	require.Equal(t, event.ID, decoded.ID)
}

func TestOnlineDeliveryPropagatesBrokerError(t *testing.T) {
	brokerErr := errors.New("broker rejected the append")
	writer := newControlledWriter()
	writer.err = brokerErr
	delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)

	err := delivery.Handle(t.Context(), onlineEnvelopeEvent(cdc.OnlineDelete, 9, nil))
	require.ErrorIs(t, err, brokerErr)
	require.Equal(t, uint64(1), delivery.snapshot().Failed)
	require.Equal(t, uint64(0), delivery.snapshot().Written)
}

func TestOnlineDeliveryTreatsCancellationAsUnknownOutcome(t *testing.T) {
	writer := newControlledWriter()
	writer.block = true
	delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := delivery.Handle(ctx, onlineEnvelopeEvent(cdc.OnlineUpdate, 3, map[string]any{"id": 3}))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "unconfirmed outcome")

	stats := delivery.snapshot()
	require.Equal(t, uint64(1), stats.Unknown, "a cancelled write is never counted as written or as a confirmed failure")
	require.Equal(t, uint64(0), stats.Written)
	require.Equal(t, uint64(0), stats.Failed)
}

func TestOnlineDeliveryFailsClosedBeforeWriting(t *testing.T) {
	oversized := newControlledWriter()
	delivery := newTestDelivery(oversized, map[string]string{"public.accounts": "accounts.topic"}, 64)
	err := delivery.Handle(t.Context(), onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1, "pad": string(make([]byte, 128))}))
	require.ErrorIs(t, err, ErrOnlineOversized)
	require.Empty(t, oversized.writtenMessages(), "an oversized envelope must never reach the broker")
	require.Equal(t, uint64(1), delivery.snapshot().Rejected)

	unmapped := newControlledWriter()
	delivery = newTestDelivery(unmapped, map[string]string{"public.other": "other.topic"}, 1<<20)
	err = delivery.Handle(t.Context(), onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1}))
	require.ErrorIs(t, err, ErrOnlineUnmappedTable)
	require.Empty(t, unmapped.writtenMessages(), "an unmapped table must never be dropped or written elsewhere")

	canceled := newControlledWriter()
	delivery = newTestDelivery(canceled, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)
	ctx, stop := context.WithCancel(t.Context())
	stop()
	require.ErrorIs(t, delivery.Handle(ctx, onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1})), context.Canceled)
	require.Empty(t, canceled.writtenMessages())
}

func TestOnlineDeliveryRejectsUnencodableEvent(t *testing.T) {
	writer := newControlledWriter()
	delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)

	event := onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1})
	event.Order.EndLSN = pq.LSN(0)
	err := delivery.Handle(t.Context(), event)
	require.Error(t, err)
	require.Empty(t, writer.writtenMessages())
	require.Equal(t, uint64(1), delivery.snapshot().Rejected)
}

// TestOnlineDeliveryHasNoAcknowledgementOwner guards the single-ACK-owner rule:
// the delivery path cannot acknowledge the replication slot itself.
func TestOnlineDeliveryHasNoAcknowledgementOwner(t *testing.T) {
	type acknowledgementOwner interface{ Ack() error }

	var delivery any = &onlineDelivery{}
	_, isOwner := delivery.(acknowledgementOwner)
	require.False(t, isOwner, "the core coordinator must stay the only source ACK owner")
}

// TestOnlineDeliveryNormalizesKafkaWriteErrors covers the REAL return shape:
// kafka.Writer reports per-message failures as kafka.WriteErrors, which has no
// Unwrap. Using it directly would lose the deadline/broker identity and
// misclassify an unconfirmed outcome as a confirmed failure.
func TestOnlineDeliveryNormalizesKafkaWriteErrors(t *testing.T) {
	brokerTooLarge := gokafka.MessageTooLargeError{}
	tests := []struct {
		name            string
		writeErr        error
		wantUnconfirmed bool
		wantIs          error
		wantAs          any
	}{
		{
			name:            "writer-level deadline",
			writeErr:        gokafka.WriteErrors{context.DeadlineExceeded},
			wantUnconfirmed: true,
			wantIs:          context.DeadlineExceeded,
		},
		{
			name:            "writer-level cancellation",
			writeErr:        gokafka.WriteErrors{context.Canceled},
			wantUnconfirmed: true,
			wantIs:          context.Canceled,
		},
		{
			name:     "broker protocol rejection",
			writeErr: gokafka.WriteErrors{gokafka.MessageSizeTooLarge},
			wantIs:   gokafka.MessageSizeTooLarge,
		},
		{
			name:     "broker message-too-large error",
			writeErr: gokafka.WriteErrors{brokerTooLarge},
			wantAs:   &gokafka.MessageTooLargeError{},
		},
		{
			name:            "empty write errors is ambiguous",
			writeErr:        gokafka.WriteErrors{},
			wantUnconfirmed: true,
			wantIs:          ErrOnlineWriteUnconfirmed,
		},
		{
			name:            "nil-only entry is ambiguous",
			writeErr:        gokafka.WriteErrors{nil},
			wantUnconfirmed: true,
			wantIs:          ErrOnlineWriteUnconfirmed,
		},
		{
			name:            "multi-entry is ambiguous for one event",
			writeErr:        gokafka.WriteErrors{context.Canceled, gokafka.MessageSizeTooLarge},
			wantUnconfirmed: true,
			wantIs:          ErrOnlineWriteUnconfirmed,
		},
		{
			// Two result entries for a one-message request, even when only one is
			// non-nil: counting non-nil entries would wrongly confirm a failure.
			name:            "broker error plus nil entry is ambiguous",
			writeErr:        gokafka.WriteErrors{gokafka.MessageSizeTooLarge, nil},
			wantUnconfirmed: true,
			wantIs:          ErrOnlineWriteUnconfirmed,
		},
		{
			name:            "nil then broker error is ambiguous",
			writeErr:        gokafka.WriteErrors{nil, context.DeadlineExceeded},
			wantUnconfirmed: true,
			wantIs:          ErrOnlineWriteUnconfirmed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := newControlledWriter()
			writer.err = test.writeErr
			delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)

			err := delivery.Handle(t.Context(), onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1}))
			require.Error(t, err)
			if test.wantAs != nil {
				require.ErrorAs(t, err, test.wantAs, "the underlying write error type must survive")
			} else {
				require.ErrorIs(t, err, test.wantIs, "the underlying write error identity must survive")
			}

			stats := delivery.snapshot()
			require.Zero(t, stats.Written, "a failed write is never counted as delivered")
			if test.wantUnconfirmed {
				require.Equal(t, uint64(1), stats.Unknown)
				require.Zero(t, stats.Failed)
				require.ErrorIs(t, err, ErrOnlineWriteUnconfirmed)
				return
			}
			require.Equal(t, uint64(1), stats.Failed)
			require.Zero(t, stats.Unknown)
			require.NotErrorIs(t, err, ErrOnlineWriteUnconfirmed)
		})
	}
}
