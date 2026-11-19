package cdc

import (
	cdc "github.com/Trendyol/go-pq-cdc"
)

// newOnlineHandlerMessage preserves decoded scalar types and isolates all
// mutable containers in the admitted scalar/JSON/one-dimensional array profile.
// READ uses HIGH as its snapshot LSN, with unavailable XID/time left zero.
func newOnlineHandlerMessage(event cdc.OnlineEvent) *Message {
	kind := MessageType(event.Operation)
	lsn := event.LSN
	if event.Operation == cdc.OnlineRead {
		kind = SnapshotMessage
		lsn = event.Order.EndLSN
	}
	return &Message{
		Type: kind, TableNamespace: event.Table.Schema, TableName: event.Table.Name,
		LSN: lsn, TransactionID: event.TransactionID, EventTime: event.EventTime,
		OldData: cloneOnlineImage(event.Before), NewData: cloneOnlineImage(event.Image),
	}
}

func cloneOnlineImage(image map[string]any) map[string]any {
	if image == nil {
		return nil
	}
	owned := make(map[string]any, len(image))
	for key, value := range image {
		owned[key] = cloneOnlineValue(value)
	}
	return owned
}

func cloneOnlineValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneOnlineImage(v)
	case []any:
		owned := make([]any, len(v))
		for i, element := range v {
			owned[i] = cloneOnlineValue(element)
		}
		return owned
	case []byte:
		return append([]byte(nil), v...)
	default:
		return value // admitted decoded scalar values are immutable
	}
}
