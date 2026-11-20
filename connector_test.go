package cdc

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka/producer"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/produce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.InitLogger(slog.Default())
	os.Exit(m.Run())
}

func TestListener_AcksMessageWithoutEventsAtOnceWhenNothingIsPending(t *testing.T) {
	tests := []struct {
		message any
		name    string
	}{
		{name: "relation", message: &format.Relation{Namespace: "public", Name: "users"}},
		{name: "truncate", message: &format.Truncate{}},
		{name: "logical decoding message", message: &format.LogicalDecodingMessage{Prefix: "audit"}},
		{name: "unmapped table", message: insertInto("orders", nil)},
		{name: "handler returns no events", message: insertInto("users", map[string]any{"skip": true})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestConnector(t)
			var acks []string

			c.listener(recordingContext(&acks, tt.name, tt.message))

			assert.Equal(t, []string{tt.name}, acks)
			assert.False(t, c.producer.HasPendingMessages())
		})
	}
}

func TestListener_AckWaitsForEarlierMessagesInBatch(t *testing.T) {
	c := newTestConnector(t)
	var acks []string

	c.listener(recordingContext(&acks, "insert", insertInto("users", nil)))
	c.listener(recordingContext(&acks, "relation", &format.Relation{Namespace: "public", Name: "orders"}))
	c.listener(recordingContext(&acks, "unmapped insert", insertInto("orders", nil)))
	c.listener(recordingContext(&acks, "skipped insert", insertInto("users", map[string]any{"skip": true})))

	assert.Empty(t, acks, "nothing may be acked before the pending insert is written")

	c.producer.ProducerBatch.FlushMessages()

	assert.Equal(t, []string{"insert", "relation", "unmapped insert", "skipped insert"}, acks)
}

func TestListener_ChunkedMessageAckedOnceAfterLastChunk(t *testing.T) {
	c := newTestConnector(t)
	c.handler = func(*Message) []gokafka.Message {
		return make([]gokafka.Message, 2*c.cfg.Kafka.ProducerBatchSize+1)
	}
	var acks []string

	c.listener(recordingContext(&acks, "fanout", insertInto("users", nil)))

	assert.Empty(t, acks, "the full chunks were flushed, the last one is still pending")
	c.producer.ProducerBatch.FlushMessages()
	assert.Equal(t, []string{"fanout"}, acks)
}

func newTestConnector(t *testing.T) *connector {
	t.Helper()

	cfg := &config.Connector{Kafka: config.Kafka{
		TableTopicMapping:           map[string]string{"public.users": "users"},
		ProducerBatchTickerDuration: time.Hour,
		ProducerBatchSize:           5,
	}}
	cfg.SetDefault()
	p, err := producer.NewProducer(stubClient{}, cfg, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	return &connector{cfg: cfg, producer: p, handler: eventUnlessSkipped}
}

func eventUnlessSkipped(msg *Message) []gokafka.Message {
	if msg.NewData["skip"] == true {
		return nil
	}
	return []gokafka.Message{{Value: []byte("row")}}
}

func insertInto(table string, data map[string]any) *format.Insert {
	return &format.Insert{TableNamespace: "public", TableName: table, Decoded: data}
}

// recordingContext returns a listener context for message whose Ack appends name to acks.
func recordingContext(acks *[]string, name string, message any) *replication.ListenerContext {
	return &replication.ListenerContext{
		Message: message,
		Ack: func() error {
			*acks = append(*acks, name)
			return nil
		},
	}
}

// stubClient hands the producer a writer whose transport accepts every write.
type stubClient struct{}

func (stubClient) Producer() *gokafka.Writer {
	return &gokafka.Writer{
		Addr:         gokafka.TCP("kafka.test:9092"),
		Transport:    acceptAllTransport{},
		BatchTimeout: time.Nanosecond,
	}
}

// acceptAllTransport reports one partition per topic and accepts every
// produce request (the writer does not wait for acks, so the reply is unread).
type acceptAllTransport struct{}

func (acceptAllTransport) RoundTrip(_ context.Context, _ net.Addr, req gokafka.Request) (gokafka.Response, error) {
	if m, ok := req.(*metadata.Request); ok {
		res := &metadata.Response{}
		for _, topic := range m.TopicNames {
			res.Topics = append(res.Topics, metadata.ResponseTopic{Name: topic, Partitions: []metadata.ResponsePartition{{}}})
		}
		return res, nil
	}
	return &produce.Response{}, nil
}
