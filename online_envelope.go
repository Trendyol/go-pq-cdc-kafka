package cdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	gokafka "github.com/segmentio/kafka-go"
)

// OnlineEnvelopeVersion is the wire version of the online row envelope. A
// consumer that does not know this version must stop instead of guessing.
const OnlineEnvelopeVersion = 1

const (
	onlineEnvelopeContentType = "application/vnd.trendyol.go-pq-cdc-kafka.online+json"
	onlineEnvelopeVersionKey  = "cdc-envelope-version"
	onlineEnvelopeIDKey       = "cdc-event-id"
)

// OnlineOrder is the delivery version of one event, separate from CommitLSN.
// Phase 0 is CDC, phase 1 is a READ taken at the dedicated boundary, so a stale
// READ can never overwrite a newer CDC row or resurrect a DELETE.
type OnlineOrder struct {
	EndLSN  string `json:"endLsn"`
	Phase   uint8  `json:"phase"`
	Ordinal uint32 `json:"ordinal"`
}

// OnlineEnvelope is the versioned Kafka value of one online row. Image carries
// the complete row (including NULLs) for READ/INSERT/UPDATE and is null for
// DELETE, whose identity and order still tombstone the row. The key is a JSON
// integer and JSON image numbers keep their source text, so a consumer must
// decode with json.Decoder.UseNumber (DecodeOnlineEnvelope does) to avoid
// routing them through float64.
type OnlineEnvelope struct {
	Image     map[string]any `json:"image"`
	ID        string         `json:"id"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	Operation string         `json:"operation"`
	CommitLSN string         `json:"commitLsn"`
	Key       int64          `json:"key"`
	Order     OnlineOrder    `json:"order"`
	Version   int            `json:"version"`
}

// Validate mirrors the core online event contract so a malformed envelope is
// refused before it reaches the broker.
func (e OnlineEnvelope) Validate() error {
	if e.Version != OnlineEnvelopeVersion {
		return fmt.Errorf("unsupported online envelope version %d", e.Version)
	}
	if e.ID == "" || e.Schema == "" || e.Table == "" {
		return errors.New("online envelope requires an id, schema and table")
	}
	operation := cdc.OnlineOperation(e.Operation)
	switch operation {
	case cdc.OnlineInsert, cdc.OnlineUpdate, cdc.OnlineDelete, cdc.OnlineRead:
	default:
		return fmt.Errorf("unsupported online envelope operation %q", e.Operation)
	}
	order, err := e.orderToken()
	if err != nil {
		return err
	}
	if order.EndLSN == 0 {
		return errors.New("online envelope requires a nonzero order endLsn")
	}
	phase := uint8(0)
	if operation == cdc.OnlineRead {
		phase = 1
	}
	if order.Phase != phase {
		return fmt.Errorf("online envelope operation %q cannot use order phase %d", e.Operation, order.Phase)
	}
	if _, err := pq.ParseLSN(e.CommitLSN); err != nil {
		return fmt.Errorf("online envelope commitLsn %q is not an LSN: %w", e.CommitLSN, err)
	}
	if operation != cdc.OnlineDelete && len(e.Image) == 0 {
		return fmt.Errorf("online envelope %q requires a complete image", e.Operation)
	}
	return nil
}

// orderToken rebuilds the core delivery version from the wire fields so phase
// and ordinal mismatches are refused here, not at the destination.
func (e OnlineEnvelope) orderToken() (cdc.OnlineOrderToken, error) {
	end, err := pq.ParseLSN(e.Order.EndLSN)
	if err != nil {
		return cdc.OnlineOrderToken{}, fmt.Errorf("online envelope order endLsn %q is not an LSN: %w", e.Order.EndLSN, err)
	}
	return cdc.OnlineOrderToken{EndLSN: end, Phase: e.Order.Phase, Ordinal: e.Order.Ordinal}, nil
}

// EncodeOnlineMessage renders the Kafka value and key for one online event.
// The value is the versioned envelope; the key is stable per schema/table/key
// so every version of a row lands on one partition for a key-preserving
// balancer. Values that JSON cannot represent fail here instead of being
// silently coerced.
func EncodeOnlineMessage(event cdc.OnlineEvent) (gokafka.Message, error) {
	envelope := NewOnlineEnvelope(event)
	if err := envelope.Validate(); err != nil {
		return gokafka.Message{}, err
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return gokafka.Message{}, fmt.Errorf("online envelope %s/%d image cannot be encoded: %w", envelope.Operation, envelope.Key, err)
	}
	return gokafka.Message{
		Key:   OnlineMessageKey(envelope.Schema, envelope.Table, envelope.Key),
		Value: value,
		Headers: []gokafka.Header{
			{Key: "content-type", Value: []byte(onlineEnvelopeContentType)},
			{Key: onlineEnvelopeVersionKey, Value: []byte(strconv.Itoa(envelope.Version))},
			{Key: onlineEnvelopeIDKey, Value: []byte(envelope.ID)},
		},
	}, nil
}

// NewOnlineEnvelope copies one core online event into its wire form. LSNs use
// the canonical PostgreSQL X/Y text form because a JSON number above 2^53 is
// not exact for every consumer; the row key stays a JSON integer that
// json.Marshal writes as exact decimal digits.
func NewOnlineEnvelope(event cdc.OnlineEvent) OnlineEnvelope {
	return OnlineEnvelope{
		Version:   OnlineEnvelopeVersion,
		ID:        event.ID,
		Schema:    event.Table.Schema,
		Table:     event.Table.Name,
		Key:       event.Key,
		Operation: string(event.Operation),
		CommitLSN: event.CommitLSN.String(),
		Order: OnlineOrder{
			EndLSN:  event.Order.EndLSN.String(),
			Phase:   event.Order.Phase,
			Ordinal: event.Order.Ordinal,
		},
		Image: event.Image,
	}
}

// DecodeOnlineEnvelope parses and validates one Kafka value. UseNumber keeps
// integer and JSON-number image values exact instead of routing them through
// float64.
func DecodeOnlineEnvelope(value []byte) (OnlineEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var envelope OnlineEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return OnlineEnvelope{}, fmt.Errorf("online envelope decode: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return OnlineEnvelope{}, errors.New("online envelope has trailing input")
	}
	if err := envelope.Validate(); err != nil {
		return OnlineEnvelope{}, err
	}
	return envelope, nil
}

// OnlineMessageKey builds the partitioned Kafka key. Components are
// length-delimited like the core event identity, so quoted dots, digits and
// underscores in schema/table names cannot collide with the key suffix.
func OnlineMessageKey(schema, table string, key int64) []byte {
	return []byte(strconv.Itoa(len(schema)) + ":" + schema + strconv.Itoa(len(table)) + ":" + table + strconv.FormatInt(key, 10))
}

// onlineMessageSize is the Kafka wire size of one message, including record
// framing and headers. It matches kafka-go's own batch accounting closely
// enough to refuse an oversized envelope before a request is attempted.
func onlineMessageSize(message gokafka.Message) int64 {
	size := int64(4 + 1 + 1 + 8) // crc, magic, attributes, timestamp
	size += int64(4 + len(message.Key))
	size += int64(4 + len(message.Value))
	size += int64(varintLen(int64(len(message.Headers))))
	for _, header := range message.Headers {
		size += int64(varintLen(int64(len(header.Key))) + len(header.Key))
		size += int64(varintLen(int64(len(header.Value))) + len(header.Value))
	}
	return size
}

// varintLen mirrors the zig-zag varint length used for Kafka header counts and
// string/bytes lengths.
func varintLen(value int64) int {
	unsigned := uint64((value << 1) ^ (value >> 63))
	length := 0
	for unsigned >= 0x80 {
		unsigned >>= 7
		length++
	}
	return length + 1
}
