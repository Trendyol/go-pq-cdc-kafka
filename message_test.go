package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
)

func TestNewMessageCarriesCommitLSN(t *testing.T) {
	const commitLSN pq.LSN = 0x16000060

	tests := []struct {
		name    string
		message any
	}{
		{name: "insert", message: &format.Insert{}},
		{name: "update", message: &format.Update{}},
		{name: "delete", message: &format.Delete{}},
		{name: "snapshot", message: &format.Snapshot{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := newMessage(tt.message, commitLSN)

			assert.Equal(t, commitLSN, msg.CommitLSN)
		})
	}
}

func TestNewMessageReturnsNilForUnsupportedMessage(t *testing.T) {
	msg := newMessage(struct{}{}, 1)

	assert.Nil(t, msg)
}
