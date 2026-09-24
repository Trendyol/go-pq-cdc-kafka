package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdckafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loggingHandler mimics a custom handler that logs write errors instead of
// panicking, which is the path where failed flushes used to lose messages.
type loggingHandler struct{}

func (loggingHandler) OnSuccess(_ *cdckafka.ResponseHandlerContext) {}
func (loggingHandler) OnError(ctx *cdckafka.ResponseHandlerContext) {
	fmt.Println("kafka write error:", ctx.Err)
}

// Kafka goes down while rows are being inserted. Flushes fail (bounded
// producer attempts) and must keep retrying; once Kafka is back every row
// must reach the topic instead of being acked past and lost.
func TestConnector_FailedFlush_RetriesUntilKafkaRecovers(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	const tableName = "failed_flush_events"
	dropTable(t, db, tableName)
	createOversizedEventsTable(t, db, tableName)

	topic := "failed.flush.test"
	cfg := oversizedConnectorConfig(
		"cdc_slot_failed_flush",
		"cdc_publication_failed_flush",
		tableName,
		topic,
		false,
		"",
		5,
	)
	cfg.Kafka.ProducerBatchTickerDuration = 500 * time.Millisecond
	cfg.Kafka.ProducerMaxAttempts = 2
	cfg.Kafka.WriteTimeout = 2 * time.Second
	cfg.Kafka.ReadTimeout = 2 * time.Second

	connector, err := cdc.NewConnector(ctx, cfg, oversizedPayloadHandler, cdc.WithResponseHandler(loggingHandler{}))
	require.NoError(t, err)
	defer connector.Close()

	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	// Warm up: prove the pipeline works and the topic exists.
	insertOversizedEvent(t, db, tableName, "before-outage", smallPayload(0))
	require.Len(t, readTopicMessages(t, topic, 1, 15*time.Second), 1)

	stopTimeout := 10 * time.Second
	require.NoError(t, Infra.KafkaContainer.Stop(ctx, &stopTimeout))
	// The broker is shared with the rest of the suite; bring it back even if
	// an assertion below fails.
	t.Cleanup(func() {
		if !Infra.KafkaContainer.IsRunning() {
			_ = Infra.KafkaContainer.Start(context.Background())
		}
	})

	const duringOutage = 12
	for i := 1; i <= duringOutage; i++ {
		insertOversizedEvent(t, db, tableName, fmt.Sprintf("during-outage-%d", i), smallPayload(i))
	}
	// Let several flushes fail while Kafka is down.
	time.Sleep(8 * time.Second)

	require.NoError(t, Infra.KafkaContainer.Start(ctx))
	waitForKafkaLeader(t, topic, 60*time.Second)

	insertOversizedEvent(t, db, tableName, "after-outage", smallPayload(99))

	// Retrying a partially written batch may re-send some messages, which
	// at-least-once allows. Read until the post-outage marker, then check
	// that nothing produced during the outage was lost.
	names := readTopicNamesUntil(t, topic, "after-outage", 90*time.Second)

	assert.Equal(t, "before-outage", names[0])
	for i := 1; i <= duringOutage; i++ {
		assert.Contains(t, names, fmt.Sprintf("during-outage-%d", i), "message produced during outage must not be lost")
	}
	assert.Equal(t, "after-outage", names[len(names)-1])
}

// waitForKafkaLeader blocks until the restarted broker serves the partition
// leader again, so a subsequent read does not hit a transient NotLeader error.
func waitForKafkaLeader(t *testing.T, topic string, timeout time.Duration) {
	t.Helper()

	addr := fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := kafka.DialLeader(dialCtx, "tcp", addr, topic, 0)
		cancel()
		if err == nil {
			_, err = conn.ReadLastOffset()
			conn.Close()
			if err == nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("kafka leader for %s not available within %s", topic, timeout)
}

// readTopicNamesUntil reads from the beginning of the topic until a message
// with the given name is seen, re-dialing on the transient errors a freshly
// restarted broker returns (NotLeader, connection resets).
func readTopicNamesUntil(t *testing.T, topic, stopAt string, timeout time.Duration) []string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			Topic:     topic,
			Partition: 0,
			MinBytes:  1,
			MaxBytes:  10e6,
		})
		_ = reader.SetOffset(kafka.FirstOffset)

		var names []string
		done := false
		readCtx, cancel := context.WithDeadline(context.Background(), deadline)
		for !done {
			message, err := reader.ReadMessage(readCtx)
			if err != nil {
				lastErr = err
				break
			}
			var data map[string]interface{}
			require.NoError(t, json.Unmarshal(message.Value, &data))
			if name, ok := data["name"].(string); ok {
				names = append(names, name)
				done = name == stopAt
			}
		}
		cancel()
		reader.Close()

		if done {
			return names
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("message %q not seen on %s within %s, last error: %v", stopAt, topic, timeout, lastErr)
	return nil
}
