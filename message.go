package cdc

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

type Message struct {
	EventTime      time.Time
	TableName      string
	TableNamespace string

	OldData map[string]any
	NewData map[string]any

	Type MessageType
}

func NewInsertMessage(m *format.Insert) *Message {
	return &Message{
		EventTime:      m.MessageTime,
		TableName:      m.TableName,
		TableNamespace: m.TableNamespace,
		OldData:        nil,
		NewData:        m.Decoded,
		Type:           InsertMessage,
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
