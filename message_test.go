package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
)

func TestNewMessageCarriesLSN(t *testing.T) {
	const lsn pq.LSN = 0x16B374D848

	tests := []struct {
		message *Message
		name    string
	}{
		{name: "insert", message: NewInsertMessage(&format.Insert{LSN: lsn})},
		{name: "update", message: NewUpdateMessage(&format.Update{LSN: lsn})},
		{name: "delete", message: NewDeleteMessage(&format.Delete{LSN: lsn})},
		{name: "snapshot", message: NewSnapshotMessage(&format.Snapshot{LSN: lsn})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, lsn, tt.message.LSN)
		})
	}
}

func TestNewMessageCarriesTransactionID(t *testing.T) {
	const xid uint32 = 4711

	tests := []struct {
		message any
		name    string
	}{
		{name: "insert", message: &format.Insert{}},
		{name: "update", message: &format.Update{}},
		{name: "delete", message: &format.Delete{}},
		{name: "snapshot", message: &format.Snapshot{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := newMessage(&replication.ListenerContext{Message: tt.message, Xid: xid})

			assert.Equal(t, xid, msg.TransactionID)
		})
	}
}

func TestNewMessageReturnsNilForUnsupportedMessage(t *testing.T) {
	msg := newMessage(&replication.ListenerContext{Message: &format.Truncate{}, Xid: 1})

	assert.Nil(t, msg)
}
