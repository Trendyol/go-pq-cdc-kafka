package cdc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	cdc "github.com/Trendyol/go-pq-cdc"
	gokafka "github.com/segmentio/kafka-go"
)

// OnlineMetadataVersion versions the reserved cdc-* record headers. Custom
// Handler values are not required to use the builtin envelope format.
const OnlineMetadataVersion = 1

// OnlineOutputID identifies a deterministic Handler output ordinal on replay.
// Handlers must preserve output order/content themselves; this is not deduplication.
func OnlineOutputID(eventID string, index int) string {
	hash := sha256.Sum256([]byte(strconv.Itoa(len(eventID)) + ":" + eventID + ":" + strconv.Itoa(index)))
	return "output:" + hex.EncodeToString(hash[:])
}

func onlineRecordMetadata(event cdc.OnlineEvent, index int) []gokafka.Header {
	return []gokafka.Header{
		{Key: "cdc-metadata-version", Value: []byte(strconv.Itoa(OnlineMetadataVersion))},
		{Key: onlineEnvelopeIDKey, Value: []byte(event.ID)},
		{Key: "cdc-order-end-lsn", Value: []byte(event.Order.EndLSN.String())},
		{Key: "cdc-order-phase", Value: []byte(strconv.Itoa(int(event.Order.Phase)))},
		{Key: "cdc-order-ordinal", Value: []byte(strconv.FormatUint(uint64(event.Order.Ordinal), 10))},
		{Key: "cdc-output-index", Value: []byte(strconv.Itoa(index))},
		{Key: "cdc-output-id", Value: []byte(OnlineOutputID(event.ID, index))},
	}
}

// prepareMessages validates the whole fanout BEFORE any write, then owns all
// byte/header buffers. One rejected later output cannot cause a partial append.
func (d *onlineDelivery) prepareMessages(event cdc.OnlineEvent, topic string) ([]gokafka.Message, error) {
	var records []gokafka.Message
	if d.handler == nil {
		message, err := EncodeOnlineMessage(event)
		if err != nil {
			return nil, err
		}
		records = []gokafka.Message{message}
	} else {
		records = d.handler(newOnlineHandlerMessage(event))
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: Handler produced no records; online filtering cannot certify delivery", ErrOnlineAdmission)
	}
	if d.maxRecords > 0 && len(records) > d.maxRecords {
		return nil, fmt.Errorf("%w: Handler fanout exceeds producerBatchSize", ErrOnlineAdmission)
	}
	owned := make([]gokafka.Message, len(records))
	var total int64
	for i, record := range records {
		record.Topic = getTopicName(topic, record.Topic)
		if d.handler != nil {
			if err := validateOnlineHandlerRecord(record); err != nil {
				return nil, err
			}
		}
		metadata := onlineRecordMetadata(event, i)
		if d.handler == nil {
			// The builtin encoder already owns this verbatim event-ID header.
			metadata = append(metadata[:1], metadata[2:]...)
		}
		size := onlineMessageSize(record)
		for _, header := range metadata {
			size += int64(varintLen(int64(len(header.Key))) + len(header.Key) + varintLen(int64(len(header.Value))) + len(header.Value))
		}
		size += int64(varintLen(int64(len(record.Headers)+len(metadata))) - varintLen(int64(len(record.Headers))))
		if size > d.maxBytes {
			return nil, fmt.Errorf("%w: output %d needs %d bytes, limit %d", ErrOnlineOversized, i, size, d.maxBytes)
		}
		if d.maxOutputBytes > 0 && size > d.maxOutputBytes-total {
			return nil, fmt.Errorf("%w: Handler fanout exceeds producerBatchBytes", ErrOnlineOversized)
		}
		total += size
		owned[i] = cloneOnlineRecord(record)
		owned[i].Headers = append(owned[i].Headers, metadata...)
	}
	return owned, nil
}

func validateOnlineHandlerRecord(record gokafka.Message) error {
	if !validOnlineTopic(record.Topic) {
		return fmt.Errorf("%w: Handler requires a valid destination topic", ErrOnlineAdmission)
	}
	if len(record.Key) == 0 || len(record.Value) == 0 {
		return fmt.Errorf("%w: Handler requires a nonempty key and value; raw tombstones are unsafe online", ErrOnlineAdmission)
	}
	for _, header := range record.Headers {
		if header.Key == "" || !utf8.ValidString(header.Key) || strings.ContainsRune(header.Key, 0) || strings.HasPrefix(strings.ToLower(header.Key), "cdc-") {
			return fmt.Errorf("%w: Handler header key is invalid or reserved (cdc-*)", ErrOnlineAdmission)
		}
	}
	return nil
}

func validOnlineTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > 249 || topic == "." || topic == ".." {
		return false
	}
	for _, r := range topic {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func cloneOnlineRecord(record gokafka.Message) gokafka.Message {
	owned := record
	owned.Key = append([]byte(nil), record.Key...)
	owned.Value = append([]byte(nil), record.Value...)
	owned.Headers = make([]gokafka.Header, len(record.Headers))
	for i, header := range record.Headers {
		owned.Headers[i] = gokafka.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return owned
}
