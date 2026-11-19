package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	gokafka "github.com/segmentio/kafka-go"
)

// ErrOnlineUnmappedTable refuses an event whose destination is not configured.
// A table without a topic is a configuration fault, never a silent skip.
var ErrOnlineUnmappedTable = errors.New("online kafka table has no destination topic")

// ErrOnlineOversized refuses an envelope that cannot fit one Kafka message. The
// source keeps its progress instead of dropping the row.
var ErrOnlineOversized = errors.New("online kafka envelope exceeds maxMessageBytes")

// ErrOnlineWriteUnconfirmed marks a write whose outcome the writer could not
// certify (cancellation, a writer-level deadline, or an ambiguous result shape).
// The source never advances for it, and it is never counted as written.
var ErrOnlineWriteUnconfirmed = errors.New("online kafka write outcome unconfirmed")

// onlineMessageWriter is the synchronous write boundary of the online path. It
// is private on purpose: there is no public way to inject a writer that reports
// success without a broker acknowledgement.
type onlineMessageWriter interface {
	WriteMessages(ctx context.Context, messages ...gokafka.Message) error
	Close() error
}

// OnlineDeliveryStats is a bounded cumulative snapshot of the delivery path.
// Unknown counts writes whose outcome the broker did not certify (cancellation
// or deadline); such a write is never reported as written and never advances
// the source.
type OnlineDeliveryStats struct {
	LastWriteLatency time.Duration
	Written          uint64
	Failed           uint64
	Unknown          uint64
	Rejected         uint64
}

type onlineDeliveryCounters struct {
	lastWriteLatency atomic.Int64
	written          atomic.Uint64
	failed           atomic.Uint64
	unknown          atomic.Uint64
	rejected         atomic.Uint64
}

// onlineDelivery is the durable online handler: it returns nil only after
// kafka.Writer.WriteMessages returned nil for acks=all. It never acknowledges
// the replication stream; the core coordinator stays the single ACK owner.
type onlineDelivery struct {
	writer         onlineMessageWriter
	counters       *onlineDeliveryCounters
	topics         map[string]string
	maxBytes       int64
	handler        Handler
	maxRecords     int
	maxOutputBytes int64
}

// Handle transforms one protected core event, validates all outputs, and writes
// each synchronously with the delivery context supplied by the core coordinator.
func (d *onlineDelivery) Handle(ctx context.Context, event cdc.OnlineEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		d.counters.rejected.Add(1)
		return fmt.Errorf("online kafka event: %w", err)
	}
	table := event.Table.Schema + "." + event.Table.Name
	topic, ok := d.topics[table]
	if !ok {
		d.counters.rejected.Add(1)
		return fmt.Errorf("%w: %s", ErrOnlineUnmappedTable, table)
	}
	messages, err := d.prepareMessages(event, topic)
	if err != nil {
		d.counters.rejected.Add(1)
		return err
	}
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.writeMessage(ctx, event, message); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (d *onlineDelivery) writeMessage(ctx context.Context, event cdc.OnlineEvent, message gokafka.Message) error {
	table := event.Table.Schema + "." + event.Table.Name
	topic := message.Topic
	started := time.Now()
	writeErr := d.writer.WriteMessages(ctx, message)
	d.counters.lastWriteLatency.Store(int64(time.Since(started)))

	if writeErr == nil {
		d.counters.written.Add(1)
		return nil
	}
	unconfirmed, cause := normalizeOnlineWriteError(writeErr)
	if unconfirmed {
		d.counters.unknown.Add(1)
		return fmt.Errorf("online kafka write %s/%d topic %s at %s has an unconfirmed outcome (%w): %w",
			table, event.Key, topic, event.Order.EndLSN.String(), ErrOnlineWriteUnconfirmed, cause)
	}
	d.counters.failed.Add(1)
	return fmt.Errorf("online kafka write %s/%d topic %s at %s: %w", table, event.Key, topic, event.Order.EndLSN.String(), cause)
}

// normalizeOnlineWriteError unwraps the shapes kafka.Writer returns for a
// single-message write. kafka.WriteErrors carries one entry per message and has
// no Unwrap, so using it directly would drop the deadline/broker identity and
// misclassify an unconfirmed outcome as a confirmed failure.
//
// A result shape that cannot describe exactly one message is ambiguous: the
// write is reported as unconfirmed so it can never be treated as delivered.
func normalizeOnlineWriteError(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	var writeErrors gokafka.WriteErrors
	if !errors.As(err, &writeErrors) {
		return isUnconfirmedOnlineWrite(err), err
	}
	// A one-message call has exactly one result entry. Counting only non-nil
	// entries would accept a two-entry result such as {brokerError, nil} as a
	// single confirmed failure, so the raw length is what decides.
	switch len(writeErrors) {
	case 1:
		if writeErrors[0] == nil {
			return true, fmt.Errorf("%w: kafka writer reported a nil per-message error", ErrOnlineWriteUnconfirmed)
		}
		return isUnconfirmedOnlineWrite(writeErrors[0]), writeErrors[0]
	case 0:
		return true, fmt.Errorf("%w: kafka writer reported no per-message error", ErrOnlineWriteUnconfirmed)
	default:
		joined := append([]error{ErrOnlineWriteUnconfirmed}, writeErrors...)
		return true, errors.Join(joined...)
	}
}

// isUnconfirmedOnlineWrite reports whether a write error leaves the append
// outcome unknown. A writer-level deadline is not proof that nothing was
// appended; kafka-go documents no guarantee after cancellation.
func isUnconfirmedOnlineWrite(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrOnlineWriteUnconfirmed)
}

func (d *onlineDelivery) close() error {
	return d.writer.Close()
}

func (d *onlineDelivery) snapshot() OnlineDeliveryStats {
	return OnlineDeliveryStats{
		Written:          d.counters.written.Load(),
		Failed:           d.counters.failed.Load(),
		Unknown:          d.counters.unknown.Load(),
		Rejected:         d.counters.rejected.Load(),
		LastWriteLatency: time.Duration(d.counters.lastWriteLatency.Load()),
	}
}
