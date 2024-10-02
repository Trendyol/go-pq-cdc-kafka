package go_pq_cdc_kafka

import (
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"time"
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

type MessageType string

const (
	InsertMessage MessageType = "INSERT"
	UpdateMessage MessageType = "UPDATE"
	DeleteMessage MessageType = "DELETE"
)

func (m MessageType) IsInsert() bool { return m == InsertMessage }
func (m MessageType) IsUpdate() bool { return m == UpdateMessage }
func (m MessageType) IsDelete() bool { return m == DeleteMessage }
