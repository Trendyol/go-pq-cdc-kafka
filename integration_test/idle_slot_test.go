package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Without heartbeat, an idle slot only advances when go-pq-cdc confirms the
// WAL end carried by the walsender's keepalives, and it does that only once
// every message handed to the listener has been acked: the Relation message
// and every row of a batch, not just the last one.
func TestConnector_IdleSlotAdvancesWithoutHeartbeat(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	t.Cleanup(func() { db.Close() })

	var version int
	require.NoError(t, db.QueryRow(`SHOW server_version_num`).Scan(&version))
	if version < 150000 {
		t.Skipf("PostgreSQL %d: the walsender reports skipped WAL through keepalives only from PostgreSQL 15", version)
	}

	const (
		tableName       = "idle_slot_events"
		noiseTable      = "idle_slot_noise"
		slotName        = "cdc_slot_idle"
		publicationName = "cdc_publication_idle"
		rows            = 5
	)
	// Start clean: a slot left behind by an earlier run would replay its WAL.
	dropSlot(t, db, slotName)
	_, err := db.Exec("DROP PUBLICATION IF EXISTS " + publicationName)
	require.NoError(t, err)
	dropTable(t, db, tableName)
	dropTable(t, db, noiseTable)
	createOversizedEventsTable(t, db, tableName)
	createOversizedEventsTable(t, db, noiseTable) // not in the publication

	// A fresh topic per run, so records of an earlier run are never read.
	topic := fmt.Sprintf("idle.slot.test.%d", time.Now().UnixNano())
	cfg := oversizedConnectorConfig(slotName, publicationName, tableName, topic, false, "", rows)
	cfg.CDC.Heartbeat = cdcconfig.HeartbeatConfig{}
	// Only the batch size triggers a flush, so all rows share one batch.
	cfg.Kafka.ProducerBatchTickerDuration = time.Hour

	connector, err := cdc.NewConnector(ctx, cfg, oversizedPayloadHandler)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		dropSlot(t, db, slotName)
	})
	go connector.Start(ctx)
	waitForConnectorReady(ctx, t, connector)

	want := make([]string, 0, rows)
	for i := 1; i <= rows; i++ {
		name := fmt.Sprintf("idle-row-%d", i)
		insertOversizedEvent(t, db, tableName, name, smallPayload(i))
		want = append(want, name)
	}
	require.Equal(t, want, messageNames(readTopicMessages(t, topic, rows)))

	// From here on only unpublished WAL is written. The connector receives no
	// message for it, so only the keepalive confirmation can move the slot.
	noiseLSN := commitUnpublishedWAL(t, db, noiseTable)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// More unpublished WAL on every poll: the walsender sends a keepalive
		// whenever it has decoded WAL the client has not reported yet.
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (name) VALUES ('noise')`, noiseTable))
		require.NoError(c, err)

		var confirmed string
		var passed bool
		require.NoError(c, db.QueryRow(
			`SELECT confirmed_flush_lsn::text, confirmed_flush_lsn > $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1`,
			slotName, noiseLSN,
		).Scan(&confirmed, &passed))
		assert.True(c, passed, "confirmed_flush_lsn %s did not pass the unpublished WAL at %s", confirmed, noiseLSN)
	}, 30*time.Second, 200*time.Millisecond)
}

// commitUnpublishedWAL commits rows into a table outside the publication and
// returns the WAL insert position right before that commit.
func commitUnpublishedWAL(t *testing.T, db *sql.DB, table string) string {
	t.Helper()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s (name) SELECT 'noise-' || g FROM generate_series(1, 1000) g`, table))
	require.NoError(t, err)
	var lsn string
	require.NoError(t, tx.QueryRow(`SELECT pg_current_wal_insert_lsn()::text`).Scan(&lsn))
	require.NoError(t, tx.Commit())
	return lsn
}

// dropSlot drops the replication slot, waiting for its walsender to exit.
func dropSlot(t *testing.T, db *sql.DB, slotName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, err := db.Exec(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slotName)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "drop replication slot %s", slotName)
}
