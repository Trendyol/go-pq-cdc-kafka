package cdc

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

type Message struct {
	EventTime      time.Time
	TableName      string
	TableNamespace string

	OldData map[string]any
	NewData map[string]any

	Type MessageType

	// LSN is the WAL position of the change and increases for a given row.
	// Snapshot events carry the LSN the snapshot was taken at, shared by every snapshot row.
	LSN pq.LSN

	// TransactionID is the top-level PostgreSQL transaction that produced the change,
	// zero for snapshot events. It is 32-bit and wraps around: it groups changes, LSN orders them.
	TransactionID uint32
}

func newMessage(ctx *replication.ListenerContext) *Message {
	var msg *Message
	switch m := ctx.Message.(type) {
	case *format.Insert:
		msg = NewInsertMessage(m)
	case *format.Update:
		msg = NewUpdateMessage(m)
	case *format.Delete:
		msg = NewDeleteMessage(m)
	case *format.Snapshot:
		msg = NewSnapshotMessage(m)
	default:
		return nil
	}

	msg.TransactionID = ctx.Xid
	return msg
}

func NewInsertMessage(m *format.Insert) *Message {
	return &Message{
		EventTime:      m.MessageTime,
		TableName:      m.TableName,
		TableNamespace: m.TableNamespace,
		OldData:        nil,
		NewData:        m.Decoded,
		Type:           InsertMessage,
		LSN:            m.LSN,
	}
}

func NewUpdateMessage(m *format.Update) *Message {
	return &Message{
		EventTime:      m.MessageTime,
		TableName:      m.TableName,
		TableNamespace: m.TableNamespace,
		OldData:        m.OldDecoded,
		NewData:        m.NewDecoded,
		Type:           UpdateMessage,
		LSN:            m.LSN,
	}
}

func NewDeleteMessage(m *format.Delete) *Message {
	return &Message{
		EventTime:      m.MessageTime,
		TableName:      m.TableName,
		TableNamespace: m.TableNamespace,
		OldData:        m.OldDecoded,
		NewData:        nil,
		Type:           DeleteMessage,
		LSN:            m.LSN,
	}
}

func NewSnapshotMessage(m *format.Snapshot) *Message {
	return &Message{
		EventTime:      m.ServerTime,
		TableName:      m.Table,
		TableNamespace: m.Schema,
		OldData:        nil,
		NewData:        m.Data,
		Type:           SnapshotMessage,
		LSN:            m.LSN,
	}
}

type MessageType string

const (
	InsertMessage   MessageType = "INSERT"
	UpdateMessage   MessageType = "UPDATE"
	DeleteMessage   MessageType = "DELETE"
	SnapshotMessage MessageType = "SNAPSHOT"
)

func (m MessageType) IsInsert() bool   { return m == InsertMessage }
func (m MessageType) IsUpdate() bool   { return m == UpdateMessage }
func (m MessageType) IsDelete() bool   { return m == DeleteMessage }
func (m MessageType) IsSnapshot() bool { return m == SnapshotMessage }
