package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdckafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingResponseHandler counts write outcomes and, unlike the default
// handler, never panics while the broker is down.
type countingResponseHandler struct {
	successes atomic.Int64
	errors    atomic.Int64
}

func (h *countingResponseHandler) OnSuccess(_ *cdckafka.ResponseHandlerContext) { h.successes.Add(1) }
func (h *countingResponseHandler) OnError(_ *cdckafka.ResponseHandlerContext)   { h.errors.Add(1) }

// Rows waiting in a batch that Kafka cannot take must hold the slot back, even
// when messages that produce nothing arrive after them: the Relation and a row
// of a published table without a topic, and a row the handler drops. A
// connector restarted on the slot must then receive those rows again.
func TestConnector_PendingBatchHoldsSlotUntilWritten(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	t.Cleanup(func() { db.Close() })

	const (
		tableName       = "pending_ack_events"
		unmappedTable   = "pending_ack_audit"
		noiseTable      = "pending_ack_noise"
		slotName        = "cdc_slot_pending_ack"
		publicationName = "cdc_publication_pending_ack"
		batchSize       = 10
	)
	// Start clean: a slot left behind by an earlier run would replay its WAL.
	dropSlot(t, db, slotName)
	_, err := db.Exec("DROP PUBLICATION IF EXISTS " + publicationName)
	require.NoError(t, err)
	for _, table := range []string{tableName, unmappedTable, noiseTable} {
		dropTable(t, db, table)
		createOversizedEventsTable(t, db, table)
	}
	// Registered first so that it runs after every connector is closed.
	t.Cleanup(func() { dropSlot(t, db, slotName) })

	topic := fmt.Sprintf("pending.ack.test.%d", time.Now().UnixNano())
	cfg := oversizedConnectorConfig(slotName, publicationName, tableName, topic, false, "", batchSize)
	cfg.CDC.Heartbeat = cdcconfig.HeartbeatConfig{}
	// Published but without a topic, so its messages produce nothing.
	cfg.CDC.Publication.Tables = append(cfg.CDC.Publication.Tables, publication.Table{
		Name:            unmappedTable,
		ReplicaIdentity: publication.ReplicaIdentityFull,
	})
	// Only a full batch flushes, so the rows below stay pending until Close.
	cfg.Kafka.ProducerBatchTickerDuration = time.Hour
	cfg.Kafka.ProducerMaxAttempts = 2
	cfg.Kafka.WriteTimeout = 2 * time.Second
	cfg.Kafka.ReadTimeout = 2 * time.Second

	var dropSeen sync.Once
	dropped := make(chan struct{})
	dropSkipRows := func(msg *cdc.Message) []kafka.Message {
		if name, _ := msg.NewData["name"].(string); strings.HasPrefix(name, "skip") {
			dropSeen.Do(func() { close(dropped) })
			return nil
		}
		return oversizedPayloadHandler(msg)
	}

	responses := &countingResponseHandler{}
	first, err := cdc.NewConnector(ctx, cfg, dropSkipRows, cdc.WithResponseHandler(responses))
	require.NoError(t, err)
	closeFirst := sync.OnceFunc(first.Close)
	t.Cleanup(closeFirst)
	go first.Start(ctx)
	waitForConnectorReady(ctx, t, first)

	// Warm up: one full batch proves the pipeline and creates the topic.
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (name) SELECT 'warmup-' || g FROM generate_series(1, %d) g`, tableName, batchSize))
	require.NoError(t, err)
	require.Len(t, readTopicMessages(t, topic, batchSize), batchSize)

	// Kafka goes down: from here on this connector cannot write anything.
	stopTimeout := 10 * time.Second
	require.NoError(t, Infra.KafkaContainer.Stop(ctx, &stopTimeout))
	// The broker is shared with the rest of the suite; bring it back even if
	// an assertion below fails.
	t.Cleanup(func() {
		if !Infra.KafkaContainer.IsRunning() {
			_ = Infra.KafkaContainer.Start(context.Background())
		}
	})

	pending := []string{"pending-1", "pending-2", "pending-3"}
	// After a restart PostgreSQL replays a transaction only if its commit
	// starts at or after confirmed_flush_lsn: the slot must not pass this.
	firstCommit := insertWithCommitLSN(t, db, tableName, pending[0])
	for _, name := range pending[1:] {
		insertOversizedEvent(t, db, tableName, name, smallPayload(0))
	}
	insertOversizedEvent(t, db, unmappedTable, "audit", smallPayload(0))
	insertOversizedEvent(t, db, tableName, "skip-1", smallPayload(0))

	// The listener handles messages in order, so once the handler sees the row
	// it drops, the unmapped table's Relation and row have been handled too.
	select {
	case <-dropped:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the connector did not receive the row the handler drops")
	}
	// Every standby reply sent from now on reports what was acked for them.
	requireStandbyReplies(t, db, slotName, time.Now(), 2)
	assert.Never(t, func() bool {
		passed, err := confirmedPast(db, slotName, firstCommit, noiseTable)
		if err != nil {
			t.Logf("check slot: %v", err)
		}
		return passed || err != nil
	}, 3*time.Second, 200*time.Millisecond, "confirmed_flush_lsn passed rows that were never written to Kafka")
	t.Logf("after the window: confirmed_flush_lsn %s, first pending commit %s",
		confirmedFlushLSN(t, db, slotName), firstCommit)

	closeFirst()
	assert.Equal(t, int64(batchSize), responses.successes.Load(), "only the warm-up rows may have been written")
	assert.Positive(t, responses.errors.Load(), "the flush at Close must have failed")

	require.NoError(t, Infra.KafkaContainer.Start(ctx))
	waitForKafkaLeader(t, topic, 60*time.Second)

	recovered := cfg
	recovered.Kafka.ProducerBatchTickerDuration = 200 * time.Millisecond
	second, err := cdc.NewConnector(ctx, recovered, dropSkipRows, cdc.WithResponseHandler(loggingHandler{}))
	require.NoError(t, err)
	t.Cleanup(sync.OnceFunc(second.Close))
	go second.Start(ctx)
	waitForConnectorReady(ctx, t, second)

	lastCommit := insertWithCommitLSN(t, db, tableName, "after-recovery")
	names := readTopicNamesUntil(t, topic, "after-recovery", 90*time.Second)
	for _, name := range pending {
		assert.Contains(t, names, name, "a row pending in the batch was lost")
	}
	assert.NotContains(t, names, "skip-1")
	counts := make(map[string]int, len(names))
	for _, name := range names {
		counts[name]++
	}
	for name, n := range counts {
		if n > 1 {
			t.Logf("duplicate: %s delivered %d times", name, n)
		}
	}

	// With everything written, the slot moves past all of it.
	require.Eventually(t, func() bool {
		passed, err := confirmedPast(db, slotName, lastCommit, noiseTable)
		return err == nil && passed
	}, 30*time.Second, 200*time.Millisecond, "confirmed_flush_lsn did not pass the last commit %s after recovery", lastCommit)
}

// insertWithCommitLSN inserts a row in its own transaction and returns the WAL
// insert position right before COMMIT, where its commit record starts.
func insertWithCommitLSN(t *testing.T, db *sql.DB, table, name string) string {
	t.Helper()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s (name, payload) VALUES ($1, $2)`, table), name, smallPayload(0))
	require.NoError(t, err)
	var lsn string
	require.NoError(t, tx.QueryRow(`SELECT pg_current_wal_insert_lsn()::text`).Scan(&lsn))
	require.NoError(t, tx.Commit())
	return lsn
}

// confirmedPast writes a row outside the publication, so the walsender has
// WAL to send keepalives for, and reports whether the slot's
// confirmed_flush_lsn has passed lsn.
func confirmedPast(db *sql.DB, slotName, lsn, noiseTable string) (bool, error) {
	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('noise')`, noiseTable)); err != nil {
		return false, err
	}
	var passed bool
	err := db.QueryRow(
		`SELECT confirmed_flush_lsn > $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1`,
		slotName, lsn,
	).Scan(&passed)
	return passed, err
}

func confirmedFlushLSN(t *testing.T, db *sql.DB, slotName string) string {
	t.Helper()

	var lsn string
	require.NoError(t, db.QueryRow(
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slotName,
	).Scan(&lsn))
	return lsn
}

// requireStandbyReplies waits until the slot's walsender has received n
// standby status replies sent after since.
func requireStandbyReplies(t *testing.T, db *sql.DB, slotName string, since time.Time, n int) {
	t.Helper()

	last := since
	for range n {
		var reply time.Time
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var r time.Time
			err := db.QueryRow(
				`SELECT r.reply_time FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1`,
				slotName,
			).Scan(&r)
			if assert.NoError(c, err) && assert.True(c, r.After(last), "no standby reply after %s", last) {
				reply = r
			}
		}, 30*time.Second, 100*time.Millisecond)
		last = reply
	}
}
