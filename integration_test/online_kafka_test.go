package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corecdc "github.com/Trendyol/go-pq-cdc"
	kafkacdc "github.com/Trendyol/go-pq-cdc-kafka"
	kafkaconfig "github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// metricPorts are handed out per connector so the core HTTP server of one live
// test cannot collide with another live test on the same host.
var onlineKafkaMetricPort atomic.Int32

const onlineKafkaDefaultMetricPort = 19090

func nextOnlineKafkaMetricPort() int {
	return onlineKafkaDefaultMetricPort + int(onlineKafkaMetricPort.Add(1))
}

func onlineKafkaDSN() string {
	return fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort)
}

func onlineKafkaBrokers() []string {
	return []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)}
}

func connectOnlineKafka(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), onlineKafkaDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// onlineKafkaSource is one isolated live fixture: its own table, topic, slot and
// publication, all cleaned up in reverse order after the connector has stopped.
type onlineKafkaSource struct {
	conn        *pgx.Conn
	t           *testing.T
	table       string
	topic       string
	slot        string
	publication string
	partitions  int32
}

// onlineKafkaNamespace gives every invocation - including -count repetitions -
// its own namespace, so a repeated run can never inherit a table, topic, slot or
// publication from an earlier one.
func onlineKafkaNamespace(t *testing.T, name string) string {
	t.Helper()
	id, err := online.NewID()
	require.NoError(t, err)
	return name + "_" + strings.ReplaceAll(id[:8], "-", "")
}

func newOnlineKafkaSource(t *testing.T, name string, partitions int32, columns string) *onlineKafkaSource {
	t.Helper()
	ctx := context.Background()
	namespace := onlineKafkaNamespace(t, name)
	source := &onlineKafkaSource{
		conn:        connectOnlineKafka(t),
		t:           t,
		table:       "online_kafka_" + namespace,
		topic:       "online.kafka." + namespace,
		slot:        "online_kafka_slot_" + namespace,
		publication: "online_kafka_pub_" + namespace,
		partitions:  partitions,
	}
	// Source progress metadata is provisioned explicitly, as core requires.
	require.NoError(t, online.Provision(ctx, source.conn, ""))

	// Every acquisition registers its own cleanup immediately, so a later
	// failure cannot leak this fixture's table, topic, slot or publication.
	_, err := source.conn.Exec(ctx, "CREATE TABLE "+source.qualified()+" ("+columns+")")
	require.NoError(t, err)
	t.Cleanup(source.dropTable)
	// Core online verifies the catalog profile: a complete image needs FULL.
	_, err = source.conn.Exec(ctx, "ALTER TABLE "+source.qualified()+" REPLICA IDENTITY FULL")
	require.NoError(t, err)

	client := &gokafka.Client{Addr: gokafka.TCP(onlineKafkaBrokers()...), Timeout: 30 * time.Second}
	response, err := client.CreateTopics(ctx, &gokafka.CreateTopicsRequest{Topics: []gokafka.TopicConfig{{
		Topic: source.topic, NumPartitions: int(partitions), ReplicationFactor: 1,
	}}})
	require.NoError(t, err)
	require.NoError(t, response.Errors[source.topic])
	t.Cleanup(source.deleteTopic)

	// The connector creates the slot/publication during Start; their cleanup is
	// registered now and runs after the connector cleanup (LIFO).
	t.Cleanup(source.dropCaptureState)
	return source
}

func (s *onlineKafkaSource) qualified() string {
	return pgx.Identifier{"public", s.table}.Sanitize()
}

// dropCaptureState removes this fixture's capture state only after its connector
// stopped and its own slot turned inactive, so no leftover slot or managed job
// poisons the shared replication-slot budget.
func (s *onlineKafkaSource) dropCaptureState() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 90*time.Second)
	defer cancel()
	if err := s.waitSlotInactive(ctx); err != nil {
		s.t.Logf("online kafka cleanup: slot %s did not become inactive: %v", s.slot, err)
	}
	if _, err := s.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{s.publication}.Sanitize()); err != nil {
		s.t.Logf("online kafka cleanup: drop publication: %v", err)
	}
	if _, err := s.conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", s.slot); err != nil {
		s.t.Logf("online kafka cleanup: drop slot: %v", err)
	}
	// Managed jobs own progress/window children; deleting them first keeps the
	// cleanup within this fixture's own rows.
	for _, statement := range []string{
		"DELETE FROM cdc_online.windows WHERE job_id IN (SELECT job_id FROM cdc_online.jobs WHERE slot_name=$1)",
		"DELETE FROM cdc_online.table_progress WHERE job_id IN (SELECT job_id FROM cdc_online.jobs WHERE slot_name=$1)",
		"DELETE FROM cdc_online.watermarks WHERE job_id IN (SELECT job_id FROM cdc_online.jobs WHERE slot_name=$1)",
	} {
		if _, err := s.conn.Exec(ctx, statement, s.slot); err != nil {
			s.t.Logf("online kafka cleanup: delete job children: %v", err)
		}
	}
	if _, err := s.conn.Exec(ctx, "DELETE FROM cdc_online.jobs WHERE slot_name=$1", s.slot); err != nil {
		s.t.Logf("online kafka cleanup: delete job metadata: %v", err)
	}
}

func (s *onlineKafkaSource) dropTable() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 60*time.Second)
	defer cancel()
	if _, err := s.conn.Exec(ctx, "DROP TABLE IF EXISTS "+s.qualified()); err != nil {
		s.t.Logf("online kafka cleanup: drop table %s: %v", s.table, err)
	}
}

func (s *onlineKafkaSource) deleteTopic() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 60*time.Second)
	defer cancel()
	client := &gokafka.Client{Addr: gokafka.TCP(onlineKafkaBrokers()...), Timeout: 30 * time.Second}
	response, err := client.DeleteTopics(ctx, &gokafka.DeleteTopicsRequest{Topics: []string{s.topic}})
	if err != nil {
		s.t.Logf("online kafka cleanup: delete topic %s: %v", s.topic, err)
		return
	}
	if topicErr := response.Errors[s.topic]; topicErr != nil {
		s.t.Logf("online kafka cleanup: delete topic %s: %v", s.topic, topicErr)
	}
}

// jobIdentity is the managed source identity that a real restart must preserve.
type onlineKafkaJob struct {
	JobID       string
	SlotBinding string
	SourceLSN   string
	Generation  int64
	Status      string
}

func (s *onlineKafkaSource) job(t *testing.T) onlineKafkaJob {
	t.Helper()
	var job onlineKafkaJob
	require.NoError(t, s.conn.QueryRow(context.Background(), `
		SELECT job_id::text, slot_binding::text, source_lsn::text, generation, status
		FROM cdc_online.jobs WHERE slot_name=$1`, s.slot).
		Scan(&job.JobID, &job.SlotBinding, &job.SourceLSN, &job.Generation, &job.Status))
	return job
}

func (s *onlineKafkaSource) waitJob(t *testing.T, timeout time.Duration) onlineKafkaJob {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var job onlineKafkaJob
		err := s.conn.QueryRow(context.Background(), `
			SELECT job_id::text, slot_binding::text, source_lsn::text, generation, status
			FROM cdc_online.jobs WHERE slot_name=$1`, s.slot).
			Scan(&job.JobID, &job.SlotBinding, &job.SourceLSN, &job.Generation, &job.Status)
		if err == nil {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("managed job for slot %s never appeared: %v", s.slot, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (s *onlineKafkaSource) waitSlotInactive(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		var active bool
		err := s.conn.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", s.slot).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("replication slot stayed active")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *onlineKafkaSource) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	_, err := s.conn.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

func (s *onlineKafkaSource) walLSN(t *testing.T) string {
	t.Helper()
	var lsn string
	require.NoError(t, s.conn.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&lsn))
	return lsn
}

func (s *onlineKafkaSource) slotLSN(t *testing.T) string {
	t.Helper()
	var lsn string
	require.NoError(t, s.conn.QueryRow(context.Background(),
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", s.slot).Scan(&lsn))
	return lsn
}

// waitSlotAtLeast waits for the source acknowledgement to reach the fence LSN,
// which can only happen after the broker acknowledged every earlier event. It
// polls in the calling goroutine because pgx.Conn is not concurrency-safe.
func (s *onlineKafkaSource) waitSlotAtLeast(t *testing.T, fence string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		var reached bool
		err := s.conn.QueryRow(context.Background(),
			"SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", s.slot, fence).Scan(&reached)
		if err == nil && reached {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %s must confirm %s after broker acknowledgements (last error %v)", s.slot, fence, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// assertSlotLSNStable proves the acknowledgement does not move for a bounded
// window, polling in the calling goroutine for the same reason.
func (s *onlineKafkaSource) assertSlotLSNStable(t *testing.T, settled string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		var unchanged bool
		require.NoError(t, s.conn.QueryRow(context.Background(),
			"SELECT confirmed_flush_lsn = $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", s.slot, settled).Scan(&unchanged))
		require.True(t, unchanged, "the source acknowledgement must not advance without a broker ack (numeric comparison)")
		time.Sleep(25 * time.Millisecond)
	}
}

// config builds a strategy-online connector config aimed at this fixture.
func (s *onlineKafkaSource) config(t *testing.T, mutate func(cfg *kafkaconfig.Connector)) kafkaconfig.Connector {
	t.Helper()
	port, err := strconv.Atoi(Infra.PostgresPort)
	require.NoError(t, err)
	cfg := kafkaconfig.Connector{
		CDC: cdcconfig.Config{
			Host:     Infra.PostgresHost,
			Port:     port,
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				Name:              s.publication,
				CreateIfNotExists: true,
				Operations:        publication.OperationOptions,
				Tables: publication.Tables{{
					Schema: "public", Name: s.table, ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{Name: s.slot, ProtoVersion: 1, CreateIfNotExists: true},
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled: true, Mode: cdcconfig.SnapshotModeInitial, Strategy: cdcconfig.SnapshotStrategyOnline,
				ChunkSize: 1, Tables: publication.Tables{{Schema: "public", Name: s.table}},
			},
			Metric: cdcconfig.MetricConfig{Port: nextOnlineKafkaMetricPort()},
		},
		Kafka: kafkaconfig.Kafka{
			Brokers:           onlineKafkaBrokers(),
			RequiredAcks:      -1,
			Balancer:          "Hash",
			TableTopicMapping: map[string]string{"public." + s.table: s.topic},
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

// onlineKafkaRun owns one started connector and always stops it first.
type onlineKafkaRun struct {
	connector kafkacdc.OnlineConnector
	cancel    context.CancelFunc
}

func startOnlineKafka(t *testing.T, cfg kafkaconfig.Connector) *onlineKafkaRun {
	t.Helper()
	connector, err := kafkacdc.NewOnlineConnector(context.Background(), cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	run := &onlineKafkaRun{connector: connector, cancel: cancel}
	t.Cleanup(func() { run.stop(t) })
	connector.Start(ctx)
	return run
}

func (r *onlineKafkaRun) waitReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, r.connector.WaitUntilReady(ctx))
}

func (r *onlineKafkaRun) waitSnapshot(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, r.connector.WaitUntilSnapshotComplete(ctx))
}

func (r *onlineKafkaRun) waitDone(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-r.connector.Done():
	case <-time.After(timeout):
		t.Fatalf("connector did not stop within %s (health %+v)", timeout, r.connector.Health())
	}
}

// stoppedErr accepts a cooperative stop (context.Canceled after Close) or nil.
func (r *onlineKafkaRun) stoppedErr(t *testing.T) {
	t.Helper()
	err := r.connector.Err()
	require.True(t, err == nil || errors.Is(err, context.Canceled), "a cooperative stop reported %v", err)
}

func (r *onlineKafkaRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	r.connector.Close()
	select {
	case <-r.connector.Done():
	case <-time.After(30 * time.Second):
		t.Error("online kafka connector did not finish Done after Close")
	}
}

// onlineKafkaRecord is one consumed Kafka record with its decoded envelope.
type onlineKafkaRecord struct {
	Envelope  kafkacdc.OnlineEnvelope
	Value     []byte
	Key       []byte
	Partition int
	Headers   []gokafka.Header
}

// readOnlineKafkaTopic reads every partition from the beginning until the
// expected count is reached or the topic stayed idle, so assertions run on real
// broker records.
func readOnlineKafkaTopic(t *testing.T, topic string, partitions int, want int, idle time.Duration) []onlineKafkaRecord {
	t.Helper()
	readers := make([]*gokafka.Reader, 0, partitions)
	for partition := 0; partition < partitions; partition++ {
		reader := gokafka.NewReader(gokafka.ReaderConfig{
			Brokers:   onlineKafkaBrokers(),
			Topic:     topic,
			Partition: partition,
			MinBytes:  1,
			MaxBytes:  10e6,
		})
		require.NoError(t, reader.SetOffset(gokafka.FirstOffset))
		t.Cleanup(func() { _ = reader.Close() })
		readers = append(readers, reader)
	}

	records := make([]onlineKafkaRecord, 0, want)
	deadline := time.Now().Add(90 * time.Second)
	lastRecord := time.Now()
	for len(records) < want && time.Now().Before(deadline) {
		progressed := false
		for _, reader := range readers {
			readCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			message, err := reader.ReadMessage(readCtx)
			cancel()
			if err != nil {
				continue
			}
			envelope, decodeErr := kafkacdc.DecodeOnlineEnvelope(message.Value)
			require.NoError(t, decodeErr, "every online record must decode and validate")
			records = append(records, onlineKafkaRecord{
				Envelope: envelope, Value: message.Value, Key: message.Key,
				Partition: message.Partition, Headers: message.Headers,
			})
			progressed = true
			lastRecord = time.Now()
		}
		if !progressed && time.Since(lastRecord) > idle {
			break
		}
	}
	return records
}

func onlineKafkaHeader(t *testing.T, record onlineKafkaRecord, key string) string {
	t.Helper()
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	t.Fatalf("record %s has no %q header", record.Envelope.ID, key)
	return ""
}

// recordsFor returns the records of one row key and one operation, which keeps
// assertions tolerant of at-least-once duplicates without weakening them.
func recordsFor(records []onlineKafkaRecord, key int64, operation corecdc.OnlineOperation) []onlineKafkaRecord {
	var matching []onlineKafkaRecord
	for _, record := range records {
		if record.Envelope.Key == key && record.Envelope.Operation == string(operation) {
			matching = append(matching, record)
		}
	}
	return matching
}

// onlineKafkaProjection is the consumer side of the contract: exact event
// identity deduplication plus an order-aware projection retaining tombstones.
type onlineKafkaProjection struct {
	conn   *pgx.Conn
	suffix string
	// orderGuard is the exact predicate under test: disabling it must let a
	// stale, lower-order event overwrite a newer one (mutation sensitivity).
	orderGuard bool
}

func newOnlineKafkaProjection(t *testing.T, name string) *onlineKafkaProjection {
	t.Helper()
	projection := &onlineKafkaProjection{conn: connectOnlineKafka(t), suffix: onlineKafkaNamespace(t, name), orderGuard: true}
	if _, err := projection.conn.Exec(context.Background(),
		`CREATE TABLE online_kafka_ledger_`+projection.suffix+` (event_id text PRIMARY KEY, content_hash text NOT NULL)`); err != nil {
		t.Fatalf("create projection ledger: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
		defer cancel()
		if _, err := projection.conn.Exec(ctx, "DROP TABLE IF EXISTS online_kafka_ledger_"+projection.suffix); err != nil {
			t.Logf("projection cleanup: drop ledger: %v", err)
		}
	})
	if _, err := projection.conn.Exec(context.Background(), `CREATE TABLE online_kafka_view_`+projection.suffix+` (
		table_schema text NOT NULL, table_name text NOT NULL, row_key bigint NOT NULL,
		image jsonb, deleted boolean NOT NULL, order_lsn pg_lsn NOT NULL, phase smallint NOT NULL, ordinal bigint NOT NULL,
		PRIMARY KEY(table_schema,table_name,row_key))`); err != nil {
		t.Fatalf("create projection view: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
		defer cancel()
		if _, err := projection.conn.Exec(ctx, "DROP TABLE IF EXISTS online_kafka_view_"+projection.suffix); err != nil {
			t.Logf("projection cleanup: drop view: %v", err)
		}
	})
	return projection
}

// Apply stores every event exactly once by identity and only moves a row
// forward in (EndLSN, phase, ordinal) order.
func (p *onlineKafkaProjection) Apply(t *testing.T, records []onlineKafkaRecord) {
	t.Helper()
	for _, record := range records {
		require.NoError(t, p.applyEnvelope(t, record.Envelope, record.Value))
	}
}

// applyEnvelope returns an error only for a reused event identity carrying
// different content; SQL faults are infrastructure assertions.
func (p *onlineKafkaProjection) applyEnvelope(t *testing.T, envelope kafkacdc.OnlineEnvelope, raw []byte) error {
	t.Helper()
	ctx := context.Background()
	digest := sha256.Sum256(raw)
	hash := hex.EncodeToString(digest[:])

	tx, err := p.conn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := tx.Exec(ctx,
		`INSERT INTO online_kafka_ledger_`+p.suffix+` (event_id, content_hash) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		envelope.ID, hash)
	require.NoError(t, err)
	if result.RowsAffected() == 0 {
		var existing string
		require.NoError(t, tx.QueryRow(ctx,
			`SELECT content_hash FROM online_kafka_ledger_`+p.suffix+` WHERE event_id=$1`, envelope.ID).Scan(&existing))
		if existing != hash {
			return fmt.Errorf("event identity %s is reused with different content", envelope.ID)
		}
		return nil
	}

	image, err := json.Marshal(envelope.Image)
	require.NoError(t, err)
	orderGuard := `
		WHERE (online_kafka_view_` + p.suffix + `.order_lsn,online_kafka_view_` + p.suffix + `.phase,online_kafka_view_` + p.suffix + `.ordinal)
			< (EXCLUDED.order_lsn,EXCLUDED.phase,EXCLUDED.ordinal)`
	if !p.orderGuard {
		orderGuard = ""
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO online_kafka_view_`+p.suffix+` (table_schema,table_name,row_key,image,deleted,order_lsn,phase,ordinal)
		VALUES ($1,$2,$3,$4::jsonb,$5,$6::pg_lsn,$7,$8)
		ON CONFLICT (table_schema,table_name,row_key) DO UPDATE
		SET image=EXCLUDED.image, deleted=EXCLUDED.deleted, order_lsn=EXCLUDED.order_lsn, phase=EXCLUDED.phase, ordinal=EXCLUDED.ordinal`+orderGuard,
		envelope.Schema, envelope.Table, envelope.Key, string(image), envelope.Operation == string(corecdc.OnlineDelete),
		envelope.Order.EndLSN, int16(envelope.Order.Phase), int64(envelope.Order.Ordinal))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	return nil
}

// onlineKafkaRow is the projected state of one row key.
type onlineKafkaRow struct {
	Image   map[string]any
	Order   string
	Deleted bool
	Phase   int16
	Ordinal int64
}

func (p *onlineKafkaProjection) row(t *testing.T, table string, key int64) onlineKafkaRow {
	t.Helper()
	var row onlineKafkaRow
	var image []byte
	require.NoError(t, p.conn.QueryRow(context.Background(), `
		SELECT image,deleted,order_lsn::text,phase,ordinal FROM online_kafka_view_`+p.suffix+`
		WHERE table_schema='public' AND table_name=$1 AND row_key=$2`, table, key).
		Scan(&image, &row.Deleted, &row.Order, &row.Phase, &row.Ordinal))
	if len(image) == 0 || string(image) == "null" {
		return row
	}
	decoder := json.NewDecoder(bytes.NewReader(image))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&row.Image))
	return row
}

// TestOnlineKafkaSnapshotAndCdcProjection proves the real path end to end: READ
// events for the snapshot, INSERT/UPDATE/DELETE from the WAL, exact values,
// per-row partition stability and a final deduplicated projection.
func TestOnlineKafkaSnapshotAndCdcProjection(t *testing.T) {
	source := newOnlineKafkaSource(t, "snapshot", 2, `id bigint PRIMARY KEY, name text NOT NULL, balance bigint NOT NULL,
		payload jsonb NOT NULL, tags jsonb NOT NULL, memo text`)
	const bigID = int64(9007199254740993) // 2^53+1: exact only as integer text
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name,balance,payload,tags,memo) VALUES
		($1,'exact',$2,'{"n":9007199254740993,"holes":[null,-1]}','[]',NULL),
		(2,'second',-9223372036854775808,'{"big":9223372036854775807}','[1,2]','kept')`, bigID, int64(9223372036854775807))

	run := startOnlineKafka(t, source.config(t, nil))
	run.waitReady(t)
	run.waitSnapshot(t)

	// One transaction per change so each commit has its own LSN.
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name,balance,payload,tags,memo) VALUES (3,'inserted',7,'{"n":3}','[]',NULL)`)
	source.exec(t, `UPDATE `+source.qualified()+` SET name='renamed',balance=8 WHERE id=$1`, bigID)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=2`)

	fence := source.walLSN(t)
	source.waitSlotAtLeast(t, fence)

	run.stop(t)
	run.stoppedErr(t)

	records := readOnlineKafkaTopic(t, source.topic, int(source.partitions), 5, 3*time.Second)
	require.GreaterOrEqual(t, len(records), 5)

	partitionByKey := map[int64]int{}
	for _, record := range records {
		require.Equal(t, kafkacdc.OnlineEnvelopeVersion, record.Envelope.Version)
		require.Equal(t, "application/vnd.trendyol.go-pq-cdc-kafka.online+json", onlineKafkaHeader(t, record, "content-type"))
		require.Equal(t, strconv.Itoa(kafkacdc.OnlineEnvelopeVersion), onlineKafkaHeader(t, record, "cdc-envelope-version"))
		require.Equal(t, record.Envelope.ID, onlineKafkaHeader(t, record, "cdc-event-id"))
		require.Equal(t, kafkacdc.OnlineMessageKey(record.Envelope.Schema, record.Envelope.Table, record.Envelope.Key), record.Key)
		if partition, seen := partitionByKey[record.Envelope.Key]; seen {
			require.Equal(t, partition, record.Partition, "one row key must stay on one partition")
		}
		partitionByKey[record.Envelope.Key] = record.Partition
	}

	// The snapshot produced READ events (phase 1) for both initial rows.
	reads := recordsFor(records, bigID, corecdc.OnlineRead)
	require.NotEmpty(t, reads)
	require.NotEmpty(t, recordsFor(records, 2, corecdc.OnlineRead))
	for _, read := range reads {
		require.EqualValues(t, 1, read.Envelope.Order.Phase)
		require.Equal(t, "0/0", read.Envelope.CommitLSN, "a READ has no source commit")
	}
	exact := reads[len(reads)-1].Envelope
	require.Equal(t, bigID, exact.Key)
	require.Equal(t, "exact", exact.Image["name"])
	require.Equal(t, json.Number("9223372036854775807"), exact.Image["balance"])
	require.Equal(t, json.Number("9007199254740993"), exact.Image["payload"].(map[string]any)["n"])
	require.Equal(t, []any{nil, json.Number("-1")}, exact.Image["payload"].(map[string]any)["holes"])
	require.Equal(t, []any{}, exact.Image["tags"])
	require.Nil(t, exact.Image["memo"])

	// CDC events carry phase 0 and the complete new image.
	inserts := recordsFor(records, 3, corecdc.OnlineInsert)
	require.NotEmpty(t, inserts)
	require.EqualValues(t, 0, inserts[0].Envelope.Order.Phase)
	require.Equal(t, "inserted", inserts[0].Envelope.Image["name"])

	updates := recordsFor(records, bigID, corecdc.OnlineUpdate)
	require.NotEmpty(t, updates)
	require.Equal(t, "renamed", updates[0].Envelope.Image["name"])
	require.Equal(t, json.Number("8"), updates[0].Envelope.Image["balance"])
	require.Contains(t, updates[0].Envelope.Image, "payload", "an UPDATE carries the full image, not a patch")

	deletes := recordsFor(records, 2, corecdc.OnlineDelete)
	require.NotEmpty(t, deletes)
	require.Nil(t, deletes[0].Envelope.Image, "DELETE keeps identity/order without inventing an image")
	require.NotEmpty(t, deletes[0].Envelope.ID)

	projection := newOnlineKafkaProjection(t, "snapshot")
	projection.Apply(t, records)

	renamed := projection.row(t, source.table, bigID)
	require.False(t, renamed.Deleted)
	require.Equal(t, "renamed", renamed.Image["name"])
	require.Equal(t, json.Number("8"), renamed.Image["balance"])
	require.True(t, projection.row(t, source.table, 2).Deleted, "the deleted row stays as a tombstone")
	inserted := projection.row(t, source.table, 3)
	require.False(t, inserted.Deleted)
	require.Equal(t, "inserted", inserted.Image["name"])

	var mismatches int
	require.NoError(t, projection.conn.QueryRow(context.Background(), `
		SELECT count(*) FROM `+source.qualified()+` s
		JOIN online_kafka_view_`+projection.suffix+` v ON v.row_key=s.id AND v.table_name='`+source.table+`'
		WHERE v.deleted OR v.image::jsonb <> to_jsonb(s)::jsonb`).Scan(&mismatches))
	require.Zero(t, mismatches, "every live row must match its projected image exactly")

	stats := run.connector.Stats()
	require.GreaterOrEqual(t, stats.Delivery.Written, uint64(len(records)))
	require.Zero(t, stats.Delivery.Failed)
	require.Zero(t, stats.Delivery.Unknown)
	require.NotZero(t, stats.Core.SafeACK, "the source acknowledgement must have advanced")
}

// TestOnlineKafkaUnreachableBrokerKeepsSourceProgress is a real broker failure:
// the writer cannot connect, so no event may be acknowledged at the source.
func TestOnlineKafkaUnreachableBrokerKeepsSourceProgress(t *testing.T) {
	source := newOnlineKafkaSource(t, "unreachable", 1, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'only')`)

	run := startOnlineKafka(t, source.config(t, func(cfg *kafkaconfig.Connector) {
		cfg.Kafka.Brokers = []string{"127.0.0.1:1"}
		cfg.Kafka.Online.MaxAttempts = 1
		cfg.Kafka.WriteTimeout = 2 * time.Second
		cfg.Kafka.ReadTimeout = 2 * time.Second
	}))
	run.waitDone(t, 90*time.Second)

	require.Error(t, run.connector.Err(), "an unreachable broker must stop the run with an error")
	health := run.connector.Health()
	require.False(t, health.Running)
	require.Equal(t, kafkacdc.OnlineStateFailed, health.State)
	require.NotEmpty(t, health.Err)

	stats := run.connector.Stats()
	require.Zero(t, stats.Delivery.Written, "no write was acknowledged")
	require.Positive(t, stats.Delivery.Failed+stats.Delivery.Unknown)

	// Source progress must not advance after the failed delivery, and the server
	// confirmation must never pass the durable job frontier. The "below attempted
	// boundary" form is proven by the blocked-write test, where the boundary can be
	// captured while the write is still in flight.
	// No attempted boundary is supplied here: an immediately refused connection
	// never allocates the READ boundary, so sampling one would be below the slot's
	// own consistent point. The stalled-write test proves that form.
	source.assertFrontierCeilings(t, source.job(t).SourceLSN, "", "after a connection-refused failure")
	source.assertSlotLSNStable(t, source.slotLSN(t), 500*time.Millisecond)
}

// TestOnlineKafkaOversizedEnvelopeFailsClosed proves an envelope larger than
// maxMessageBytes stops delivery instead of being dropped or truncated.
func TestOnlineKafkaOversizedEnvelopeFailsClosed(t *testing.T) {
	source := newOnlineKafkaSource(t, "oversized", 1, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'small')`)

	run := startOnlineKafka(t, source.config(t, func(cfg *kafkaconfig.Connector) {
		// The one-row READ envelope fits; the wide CDC row below cannot.
		cfg.Kafka.MaxMessageBytes = "1kb"
		cfg.Kafka.Online.MaxAttempts = 1
	}))
	run.waitReady(t)
	run.waitSnapshot(t)

	beforeInsert := source.walLSN(t)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (2, repeat('x', 4096))`)
	run.waitDone(t, 90*time.Second)

	require.Error(t, run.connector.Err())
	require.ErrorIs(t, run.connector.Err(), kafkacdc.ErrOnlineOversized)
	oversizedStats := run.connector.Stats()
	require.GreaterOrEqual(t, oversizedStats.Delivery.Rejected, uint64(1))
	// Only the one-row READ was written; the oversized row never was.
	require.Equal(t, uint64(1), oversizedStats.Delivery.Written)
	require.Zero(t, oversizedStats.Delivery.Failed)

	// The oversized row is not in the topic and the source did not advance past it.
	for _, record := range readOnlineKafkaTopic(t, source.topic, int(source.partitions), 2, 2*time.Second) {
		require.NotEqual(t, int64(2), record.Envelope.Key, "the oversized row must never be silently delivered")
	}
	var behind bool
	require.NoError(t, source.conn.QueryRow(context.Background(), `SELECT $1::pg_lsn < $2::pg_lsn`,
		source.slotLSN(t), beforeInsert).Scan(&behind))
	require.True(t, behind, "confirmed_flush_lsn must stay behind the unacknowledged insert")
}

// TestOnlineKafkaReplayKeepsIdentityOrderAndTombstone proves duplicate delivery
// is idempotent and that an older READ replayed after a DELETE cannot resurrect
// the row.
func TestOnlineKafkaReplayKeepsIdentityOrderAndTombstone(t *testing.T) {
	source := newOnlineKafkaSource(t, "replay", 2, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'doomed')`)

	run := startOnlineKafka(t, source.config(t, nil))
	run.waitReady(t)
	run.waitSnapshot(t)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=1`)

	fence := source.walLSN(t)
	source.waitSlotAtLeast(t, fence)
	run.stop(t)

	records := readOnlineKafkaTopic(t, source.topic, int(source.partitions), 2, 3*time.Second)
	require.GreaterOrEqual(t, len(records), 2)

	projection := newOnlineKafkaProjection(t, "replay")
	projection.Apply(t, records)
	first := projection.row(t, source.table, 1)
	require.True(t, first.Deleted)
	require.Nil(t, first.Image)

	// Replaying the identical log must not change the projection.
	projection.Apply(t, records)
	second := projection.row(t, source.table, 1)
	require.True(t, second.Deleted)
	require.Equal(t, first.Order, second.Order)
	require.Equal(t, first.Phase, second.Phase)
	require.Equal(t, first.Ordinal, second.Ordinal)

	reads := recordsFor(records, 1, corecdc.OnlineRead)
	deletes := recordsFor(records, 1, corecdc.OnlineDelete)
	require.NotEmpty(t, reads)
	require.NotEmpty(t, deletes)
	require.EqualValues(t, 1, reads[0].Envelope.Order.Phase)
	require.EqualValues(t, 0, deletes[0].Envelope.Order.Phase)
	require.Less(t, reads[0].Envelope.Order.EndLSN, deletes[0].Envelope.Order.EndLSN,
		"the READ was taken before the DELETE")

	// Re-applying the older READ (a realistic duplicate/stale delivery) must not
	// resurrect the row: the order guard keeps the tombstone.
	require.NoError(t, projection.applyEnvelope(t, reads[0].Envelope, reads[0].Value))
	afterStaleRead := projection.row(t, source.table, 1)
	require.True(t, afterStaleRead.Deleted, "a stale READ must not resurrect a deleted row")
	require.Nil(t, afterStaleRead.Image)
	require.Equal(t, deletes[0].Envelope.Order.EndLSN, afterStaleRead.Order)

	// The same identity must never describe different content.
	tampered := reads[0].Envelope
	tampered.Image = map[string]any{"id": json.Number("1"), "name": "tampered"}
	tamperedRaw, err := json.Marshal(tampered)
	require.NoError(t, err)
	require.Error(t, projection.applyEnvelope(t, tampered, tamperedRaw),
		"an identity reused with different content must be refused")
}

// stalledEndpoint accepts TCP connections and never answers: a real socket that
// models a broker stalling after the request was written.
type stalledEndpoint struct {
	listener net.Listener
	accepted chan struct{}
	mu       sync.Mutex
	conns    []net.Conn
	once     sync.Once
}

func newStalledEndpoint(t *testing.T) *stalledEndpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := &stalledEndpoint{listener: listener, accepted: make(chan struct{})}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			endpoint.mu.Lock()
			endpoint.conns = append(endpoint.conns, conn)
			endpoint.mu.Unlock()
			endpoint.once.Do(func() { close(endpoint.accepted) })
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		endpoint.mu.Lock()
		defer endpoint.mu.Unlock()
		for _, conn := range endpoint.conns {
			_ = conn.Close()
		}
	})
	return endpoint
}

func (e *stalledEndpoint) addr() string { return e.listener.Addr().String() }

func (e *stalledEndpoint) waitAccepted(t *testing.T) {
	t.Helper()
	select {
	case <-e.accepted:
	case <-time.After(30 * time.Second):
		t.Fatal("the writer never reached the stalled broker endpoint")
	}
}

// assertFrontierCeilings proves the CAUSAL CEILING of both durable positions
// while an event is unacknowledged. All comparisons are numeric pg_lsn:
//
//	job.source_lsn        <= durableCeiling   (metadata bounded)
//	confirmed_flush_lsn   <= durableCeiling   (server bounded by the same frontier)
//	job.source_lsn        <= attemptedBoundary          (when supplied)
//	confirmed_flush_lsn   <= attemptedBoundary          (when supplied)
//
// attemptedBoundary is empty when the failing delivery left no sampleable
// boundary (an immediately refused broker never allocates the READ boundary); the
// stalled-write test supplies a genuinely captured in-flight boundary instead.
//
// The managed job checkpoint is written by core only after the handler returned
// nil, so acknowledging on enqueue would break this. The server confirmation is
// NOT ACK-gated: it follows PostgreSQL standby status updates, so it may lag and
// then catch up to the already-durable job frontier. Catch-up is legitimate; a
// value PAST the durable frontier or past the attempted boundary is not. Treating
// server equality to a sampled value as required was an over-strict test, not an
// unsafe-source finding.
func (s *onlineKafkaSource) assertFrontierCeilings(t *testing.T, durableCeiling, attemptedBoundary, situation string) {
	t.Helper()
	var jobBounded, jobBelowAttempt, serverBelowCeiling, serverBelowAttempt bool
	require.NoError(t, s.conn.QueryRow(context.Background(),
		"SELECT source_lsn <= $2::pg_lsn FROM cdc_online.jobs WHERE slot_name=$1", s.slot, durableCeiling).Scan(&jobBounded))
	require.True(t, jobBounded, "%s: cdc_online.jobs.source_lsn advanced past the durable ceiling %s", situation, durableCeiling)

	require.NoError(t, s.conn.QueryRow(context.Background(),
		"SELECT confirmed_flush_lsn <= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", s.slot, durableCeiling).Scan(&serverBelowCeiling))
	require.True(t, serverBelowCeiling, "%s: server confirmed_flush_lsn passed the durable job ceiling %s", situation, durableCeiling)

	if attemptedBoundary != "" {
		require.NoError(t, s.conn.QueryRow(context.Background(),
			"SELECT source_lsn <= $2::pg_lsn FROM cdc_online.jobs WHERE slot_name=$1", s.slot, attemptedBoundary).Scan(&jobBelowAttempt))
		require.True(t, jobBelowAttempt, "%s: cdc_online.jobs.source_lsn reached the attempted boundary %s", situation, attemptedBoundary)

		require.NoError(t, s.conn.QueryRow(context.Background(),
			"SELECT confirmed_flush_lsn <= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", s.slot, attemptedBoundary).Scan(&serverBelowAttempt))
		require.True(t, serverBelowAttempt, "%s: server confirmed_flush_lsn reached the attempted boundary %s", situation, attemptedBoundary)
	}

	t.Logf("%s: durable ceiling %s, attempted boundary %s, job %s, server %s",
		situation, durableCeiling, attemptedBoundary, s.job(t).SourceLSN, s.slotLSN(t))
}

// TestOnlineKafkaBlockedBrokerFreezesSourceFrontier is the causal gate: with a
// real stalled broker socket and an unacknowledged write in flight, BOTH durable
// frontiers must stay frozen, during the stall and after the aborted stop.
func TestOnlineKafkaBlockedBrokerFreezesSourceFrontier(t *testing.T) {
	source := newOnlineKafkaSource(t, "blocked", 1, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'blocked')`)
	endpoint := newStalledEndpoint(t)

	run := startOnlineKafka(t, source.config(t, func(cfg *kafkaconfig.Connector) {
		cfg.Kafka.Brokers = []string{endpoint.addr()}
		cfg.Kafka.WriteTimeout = time.Minute
		cfg.Kafka.ReadTimeout = time.Minute
		cfg.Kafka.Online.MaxAttempts = 1
		cfg.Kafka.Online.CloseTimeout = 3 * time.Second
	}))

	source.waitJob(t, 60*time.Second)
	endpoint.waitAccepted(t)

	// The write is now in flight and unacknowledged. Capture the allowed durable
	// frontier AT delivery: an implementation that acknowledged on enqueue would
	// advance both values while the broker has not answered.
	job := source.job(t)
	attemptedBoundary := source.walLSN(t)
	for deadline := time.Now().Add(1500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		require.NotEqual(t, kafkacdc.OnlineStateFailed, run.connector.Health().State, "connector stopped before the blocked write was released")
		source.assertFrontierCeilings(t, job.SourceLSN, attemptedBoundary, "during a blocked unacknowledged write")
	}

	run.stop(t)
	source.assertFrontierCeilings(t, job.SourceLSN, attemptedBoundary, "after the aborted stop")

	stats := run.connector.Stats()
	require.Zero(t, stats.Delivery.Written, "a blocked write is never counted as delivered")
	require.Positive(t, stats.Delivery.Unknown, "an aborted write is an unconfirmed outcome")
	require.Zero(t, stats.Delivery.Failed)
}

// TestOnlineKafkaSuccessfulAckAdvancesSourceFrontier is the positive direction of
// the same causal gate: after a real broker acknowledgement both frontiers pass
// the delivered boundary.
func TestOnlineKafkaSuccessfulAckAdvancesSourceFrontier(t *testing.T) {
	source := newOnlineKafkaSource(t, "advance", 1, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'first')`)

	run := startOnlineKafka(t, source.config(t, nil))
	run.waitReady(t)
	run.waitSnapshot(t)
	before := source.job(t)

	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (2,'second')`)
	fence := source.walLSN(t)
	source.waitSlotAtLeast(t, fence)

	after := source.job(t)
	require.Equal(t, before.JobID, after.JobID, "a running connector keeps one managed job")
	require.NotEqual(t, before.SourceLSN, after.SourceLSN, "a broker-acknowledged event must advance durable metadata")

	var jobAdvanced, slotAdvanced bool
	require.NoError(t, source.conn.QueryRow(context.Background(),
		"SELECT source_lsn >= $2::pg_lsn FROM cdc_online.jobs WHERE slot_name=$1", source.slot, fence).Scan(&jobAdvanced))
	require.True(t, jobAdvanced, "cdc_online.jobs.source_lsn must pass the acknowledged boundary")
	require.NoError(t, source.conn.QueryRow(context.Background(),
		"SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", source.slot, fence).Scan(&slotAdvanced))
	require.True(t, slotAdvanced)

	run.stop(t)
	require.NotZero(t, run.connector.Stats().Delivery.Written)
}

// TestOnlineKafkaUnseenStaleReadCannotResurrect proves the ORDER predicate, not
// duplicate-ID handling, protects a tombstone: the READ has never been applied to
// this projection, and it carries a lower numeric order than the DELETE.
func TestOnlineKafkaUnseenStaleReadCannotResurrect(t *testing.T) {
	source := newOnlineKafkaSource(t, "order", 2, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'doomed')`)

	run := startOnlineKafka(t, source.config(t, nil))
	run.waitReady(t)
	run.waitSnapshot(t)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=1`)
	source.waitSlotAtLeast(t, source.walLSN(t))
	run.stop(t)

	records := readOnlineKafkaTopic(t, source.topic, int(source.partitions), 2, 3*time.Second)
	reads := recordsFor(records, 1, corecdc.OnlineRead)
	deletes := recordsFor(records, 1, corecdc.OnlineDelete)
	require.NotEmpty(t, reads)
	require.NotEmpty(t, deletes)

	readOrder, err := pq.ParseLSN(reads[0].Envelope.Order.EndLSN)
	require.NoError(t, err)
	deleteOrder, err := pq.ParseLSN(deletes[0].Envelope.Order.EndLSN)
	require.NoError(t, err)
	require.Less(t, uint64(readOrder), uint64(deleteOrder), "numeric LSN order must place the READ before the DELETE")

	// Apply ONLY the DELETE: the READ's identity has never been seen here, so a
	// duplicate-ID short-circuit cannot be what keeps the tombstone.
	guarded := newOnlineKafkaProjection(t, "order_guarded")
	require.NoError(t, guarded.applyEnvelope(t, deletes[0].Envelope, deletes[0].Value))
	require.True(t, guarded.row(t, source.table, 1).Deleted)
	require.NoError(t, guarded.applyEnvelope(t, reads[0].Envelope, reads[0].Value))
	stale := guarded.row(t, source.table, 1)
	require.True(t, stale.Deleted, "an unseen, lower-order READ must not resurrect a deleted row")
	require.Nil(t, stale.Image)
	require.Equal(t, deletes[0].Envelope.Order.EndLSN, stale.Order)

	// Mutation sensitivity: disabling ONLY the order predicate must let the same
	// event overwrite the tombstone. The switch is a test-only field and touches
	// nothing else, so the source tree stays byte-identical.
	ungartered := newOnlineKafkaProjection(t, "order_mutated")
	ungartered.orderGuard = false
	require.NoError(t, ungartered.applyEnvelope(t, deletes[0].Envelope, deletes[0].Value))
	require.NoError(t, ungartered.applyEnvelope(t, reads[0].Envelope, reads[0].Value))
	resurrected := ungartered.row(t, source.table, 1)
	require.False(t, resurrected.Deleted, "mutation negative: without the order predicate the tombstone is overwritten")
	require.NotNil(t, resurrected.Image)
}

// TestOnlineKafkaSourceRestartPreservesJobIdentityOrderAndTombstones is a REAL
// connector restart: a new instance resumes the same managed job, slot and Kafka
// destination, preserving identities, order and tombstones. Consumer-log replay
// is covered separately by TestOnlineKafkaReplayKeepsIdentityOrderAndTombstone.
func TestOnlineKafkaSourceRestartPreservesJobIdentityOrderAndTombstones(t *testing.T) {
	source := newOnlineKafkaSource(t, "restart", 2, `id bigint PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (1,'first')`)
	cfg := source.config(t, nil)

	first := startOnlineKafka(t, cfg)
	first.waitReady(t)
	first.waitSnapshot(t)
	source.exec(t, `UPDATE `+source.qualified()+` SET name='updated' WHERE id=1`)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (2,'gone')`)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=2`)
	source.waitSlotAtLeast(t, source.walLSN(t))
	first.stop(t)
	first.stoppedErr(t)

	jobBefore := source.job(t)
	recordsBefore := readOnlineKafkaTopic(t, source.topic, int(source.partitions), 4, 3*time.Second)
	require.GreaterOrEqual(t, len(recordsBefore), 4)

	// A new connector instance on the SAME slot, publication, job and topic.
	second := startOnlineKafka(t, cfg)
	second.waitReady(t)
	source.exec(t, `INSERT INTO `+source.qualified()+` (id,name) VALUES (3,'after-restart')`)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=1`)
	restartFence := source.walLSN(t)
	source.waitSlotAtLeast(t, restartFence)
	second.stop(t)

	jobAfter := source.job(t)
	require.Equal(t, jobBefore.JobID, jobAfter.JobID, "restart must resume the same managed job")
	require.Equal(t, jobBefore.SlotBinding, jobAfter.SlotBinding, "the slot binding must stay stable")
	require.GreaterOrEqual(t, jobAfter.Generation, jobBefore.Generation)
	var metadataAdvanced bool
	require.NoError(t, source.conn.QueryRow(context.Background(),
		"SELECT source_lsn >= $2::pg_lsn FROM cdc_online.jobs WHERE slot_name=$1", source.slot, restartFence).Scan(&metadataAdvanced))
	require.True(t, metadataAdvanced, "post-restart acknowledged events must advance durable metadata")

	records := readOnlineKafkaTopic(t, source.topic, int(source.partitions), len(recordsBefore)+2, 3*time.Second)
	require.GreaterOrEqual(t, len(records), len(recordsBefore)+2)

	// Every pre-restart identity is still present with identical content, and the
	// post-restart records use the same stream (no new job/generation identity).
	beforeByID := map[string]string{}
	for _, record := range recordsBefore {
		beforeByID[record.Envelope.ID] = string(record.Value)
	}
	for _, record := range records {
		if want, ok := beforeByID[record.Envelope.ID]; ok {
			require.Equal(t, want, string(record.Value), "a replayed identity must carry identical content")
		}
	}
	require.NotEmpty(t, recordsFor(records, 3, corecdc.OnlineInsert), "post-restart INSERT must be captured")
	require.NotEmpty(t, recordsFor(records, 1, corecdc.OnlineDelete), "post-restart DELETE must be captured")

	// Final projection: pre-restart tombstones retained, post-restart state wins.
	projection := newOnlineKafkaProjection(t, "restart")
	projection.Apply(t, records)
	require.True(t, projection.row(t, source.table, 1).Deleted, "row 1 is deleted after restart")
	require.True(t, projection.row(t, source.table, 2).Deleted, "the pre-restart tombstone is retained")
	live := projection.row(t, source.table, 3)
	require.False(t, live.Deleted)
	require.Equal(t, "after-restart", live.Image["name"])
}
