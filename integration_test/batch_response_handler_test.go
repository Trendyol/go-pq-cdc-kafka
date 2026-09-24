package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdckafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchHandler mimics an outbox publisher that deletes rows per Kafka batch.
type batchHandler struct {
	mu        sync.Mutex
	batches   [][]string
	successes int
	errors    int
}

func (h *batchHandler) OnSuccess(_ *cdckafka.ResponseHandlerContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.successes++
}

func (h *batchHandler) OnError(_ *cdckafka.ResponseHandlerContext) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors++
}

func (h *batchHandler) OnBatchSuccess(messages []*kafka.Message) {
	keys := make([]string, 0, len(messages))
	for _, m := range messages {
		keys = append(keys, string(m.Key))
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.batches = append(h.batches, keys)
}

func (h *batchHandler) snapshot() (batches [][]string, successes, errors int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]string(nil), h.batches...), h.successes, h.errors
}

func TestConnector_BatchResponseHandler_ReceivesEveryWrittenMessageOncePerFlush(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "batch_handler_events"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "batch.handler.test"
	const batchSize = 5
	cfg := oversizedConnectorConfig(
		"cdc_slot_batch_handler",
		"cdc_publication_batch_handler",
		tableName,
		topic,
		false,
		"",
		batchSize,
	)

	handler := &batchHandler{}
	connector, err := cdc.NewConnector(ctx, cfg, oversizedPayloadHandler, cdc.WithResponseHandler(handler))
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	const total = 12
	for i := 1; i <= total; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("event-%d", i), smallPayload(i))
	}

	messages := readTopicMessages(t, topic, total, 15*time.Second)
	require.Len(t, messages, total)

	// Callbacks fire before ack; give the trailing ticker flush time to run.
	require.Eventually(t, func() bool {
		batches, _, _ := handler.snapshot()
		n := 0
		for _, b := range batches {
			n += len(b)
		}
		return n == total
	}, 10*time.Second, 100*time.Millisecond, "OnBatchSuccess must be called for every written message")

	batches, successes, errors := handler.snapshot()
	assert.Zero(t, successes, "per-message OnSuccess must not be called when OnBatchSuccess is implemented")
	assert.Zero(t, errors)

	// Keys are the row ids; they must arrive in producer order across batches.
	expected := make([]string, 0, total)
	for _, m := range messages {
		expected = append(expected, kafkaKeyFromData(m))
	}
	got := make([]string, 0, total)
	for _, b := range batches {
		assert.NotEmpty(t, b)
		assert.LessOrEqual(t, len(b), batchSize)
		got = append(got, b...)
	}
	assert.Equal(t, expected, got)
	assert.Less(t, len(batches), total, "batching must yield fewer callbacks than messages")
}
