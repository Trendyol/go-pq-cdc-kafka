package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	kafkacdc "github.com/Trendyol/go-pq-cdc-kafka"
	kafkaconfig "github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func startExistingConstructorKafka(t *testing.T, cfg kafkaconfig.Connector, handler kafkacdc.Handler) *onlineKafkaRun {
	t.Helper()
	connector, err := kafkacdc.NewConnector(t.Context(), cfg, handler)
	require.NoError(t, err)
	lifecycle, ok := connector.(kafkacdc.OnlineConnector)
	require.True(t, ok, "existing constructor must expose online lifecycle")
	ctx, cancel := context.WithCancel(t.Context())
	run := &onlineKafkaRun{connector: lifecycle, cancel: cancel}
	t.Cleanup(func() { run.stop(t) })
	connector.Start(ctx)
	return run
}

type existingHandlerPayload struct {
	Before    map[string]any       `json:"before"`
	After     map[string]any       `json:"after"`
	Operation kafkacdc.MessageType `json:"operation"`
	LSN       string               `json:"lsn"`
	XID       uint32               `json:"xid"`
	EventTime time.Time            `json:"eventTime"`
}

func transformExistingHandler(message *kafkacdc.Message) []gokafka.Message {
	image := message.NewData
	if message.Type.IsDelete() {
		image = message.OldData
	}
	value, err := json.Marshal(existingHandlerPayload{Before: message.OldData, After: message.NewData, Operation: message.Type, LSN: message.LSN.String(), XID: message.TransactionID, EventTime: message.EventTime})
	if err != nil {
		panic(fmt.Errorf("finite canary transformation: %w", err))
	}
	return []gokafka.Message{{Key: []byte(fmt.Sprint(image["id"])), Value: value, Headers: []gokafka.Header{{Key: "canary-format", Value: []byte("custom-v1")}}}}
}

func readExistingHandlerRecords(t *testing.T, source *onlineKafkaSource, count int) []gokafka.Message {
	t.Helper()
	reader := gokafka.NewReader(gokafka.ReaderConfig{Brokers: onlineKafkaBrokers(), Topic: source.topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	require.NoError(t, reader.SetOffset(gokafka.FirstOffset))
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	records := make([]gokafka.Message, 0, count)
	for len(records) < count {
		record, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		records = append(records, record)
	}
	return records
}

func TestExistingConstructorHandlerRealKafkaSnapshotInsertUpdateDeleteMetadata(t *testing.T) {
	source := newOnlineKafkaSource(t, "existing_handler", 1, `id integer PRIMARY KEY, name text NOT NULL, small smallint, big bigint`)
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'snapshot',3,9007199254740993)`)
	observed := make(chan *kafkacdc.Message, 16)
	run := startExistingConstructorKafka(t, source.config(t, nil), func(message *kafkacdc.Message) []gokafka.Message {
		observed <- message
		return transformExistingHandler(message)
	})
	run.waitReady(t)
	run.waitSnapshot(t)
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(2,'inserted',4,9007199254740995)`)
	source.exec(t, `UPDATE `+source.qualified()+` SET name='updated' WHERE id=2`)
	source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=2`)
	source.waitSlotAtLeast(t, source.walLSN(t))
	run.stop(t)
	run.stoppedErr(t)
	records := readExistingHandlerRecords(t, source, 4)
	operations := []kafkacdc.MessageType{kafkacdc.SnapshotMessage, kafkacdc.InsertMessage, kafkacdc.UpdateMessage, kafkacdc.DeleteMessage}
	for i, record := range records {
		message := <-observed
		require.Equal(t, operations[i], message.Type)
		image := message.NewData
		if message.Type.IsDelete() {
			image = message.OldData
		}
		require.IsType(t, int32(0), image["id"])
		require.IsType(t, int16(0), image["small"])
		require.IsType(t, int64(0), image["big"])
		var payload existingHandlerPayload
		require.NoError(t, json.Unmarshal(record.Value, &payload))
		require.Equal(t, operations[i], payload.Operation, "custom payload must not be replaced with builtin envelope")
		headers := map[string]string{}
		for _, h := range record.Headers {
			headers[h.Key] = string(h.Value)
		}
		require.Equal(t, "custom-v1", headers["canary-format"])
		require.Equal(t, "1", headers["cdc-metadata-version"])
		require.Equal(t, "0", headers["cdc-output-index"])
		require.Equal(t, kafkacdc.OnlineOutputID(headers["cdc-event-id"], 0), headers["cdc-output-id"])
		require.NotEmpty(t, headers["cdc-event-id"])
		if i == 0 {
			require.Equal(t, headers["cdc-order-end-lsn"], message.LSN.String())
			require.True(t, message.EventTime.IsZero())
			require.Zero(t, message.TransactionID)
			require.Nil(t, message.OldData)
			require.Equal(t, "1", headers["cdc-order-phase"])
		} else {
			require.Positive(t, message.TransactionID)
			require.False(t, message.EventTime.IsZero())
			require.Positive(t, message.LSN)
			require.Equal(t, "0", headers["cdc-order-phase"])
			if i == 1 {
				require.Nil(t, message.OldData)
			}
			if i == 2 {
				require.Equal(t, "inserted", message.OldData["name"])
				require.Equal(t, "updated", message.NewData["name"])
			}
			if i == 3 {
				require.Equal(t, "updated", message.OldData["name"])
				require.Nil(t, message.NewData)
			}
		}
	}
	require.Equal(t, uint64(4), run.connector.Stats().Delivery.Written)
}

func TestExistingConstructorHandlerFailClosedAfterRealDurableSnapshot(t *testing.T) {
	for _, mode := range []string{"empty_filter", "invalid_later_record", "partial_broker_failure"} {
		t.Run(mode, func(t *testing.T) {
			source := newOnlineKafkaSource(t, mode, 1, `id integer PRIMARY KEY, name text NOT NULL`)
			source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'durable snapshot')`)
			attempted := make(chan pq.LSN, 1)
			run := startExistingConstructorKafka(t, source.config(t, func(cfg *kafkaconfig.Connector) {
				cfg.Kafka.Online.MaxAttempts = 1
				cfg.Kafka.WriteTimeout = 3 * time.Second
			}), func(message *kafkacdc.Message) []gokafka.Message {
				records := transformExistingHandler(message)
				if !message.Type.IsInsert() {
					return records
				}
				attempted <- message.LSN
				switch mode {
				case "empty_filter":
					return nil
				case "invalid_later_record":
					return append(records, gokafka.Message{Key: []byte("key"), Value: []byte("value"), Topic: "invalid/topic"})
				default:
					return append(records, gokafka.Message{Key: []byte("key"), Value: []byte("value"), Topic: "missing." + source.topic})
				}
			})
			run.waitReady(t)
			run.waitSnapshot(t)
			source.waitSlotAtLeast(t, source.walLSN(t))
			source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(2,'must not acknowledge event')`)
			var rowLSN pq.LSN
			select {
			case rowLSN = <-attempted:
			case <-time.After(30 * time.Second):
				t.Fatal("Handler never observed failing source change")
			}
			run.waitDone(t, 60*time.Second)
			require.Error(t, run.connector.Err())
			require.GreaterOrEqual(t, run.connector.Stats().Delivery.Written, uint64(1), "prior READ is actually durable; not an all-zero ACK test")
			var jobBelow, serverBelow bool
			require.NoError(t, source.conn.QueryRow(t.Context(), `SELECT source_lsn < $2::pg_lsn FROM cdc_online.jobs WHERE slot_name=$1`, source.slot, rowLSN.String()).Scan(&jobBelow))
			require.NoError(t, source.conn.QueryRow(t.Context(), `SELECT confirmed_flush_lsn < $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1`, source.slot, rowLSN.String()).Scan(&serverBelow))
			require.True(t, jobBelow, "partial/empty/invalid Handler output must not certify source transaction")
			require.True(t, serverBelow, "server feedback must remain below failing real WALStart")
			count := 1
			if mode == "partial_broker_failure" {
				count = 2
				require.Equal(t, uint64(2), run.connector.Stats().Delivery.Written, "first fanout output appended, second failed")
			}
			records := readExistingHandlerRecords(t, source, count)
			require.Equal(t, []byte("1"), records[0].Key)
			if count == 2 {
				require.Equal(t, []byte("2"), records[1].Key)
			}
			t.Logf("mode=%s attempted=%s job=%s server=%s written=%d", mode, rowLSN, source.job(t).SourceLSN, source.slotLSN(t), run.connector.Stats().Delivery.Written)
		})
	}
}

func TestExistingConstructorHandlerCancellationDuringRealSocketWriteKeepsCeiling(t *testing.T) {
	source := newOnlineKafkaSource(t, "existing_cancel", 1, `id integer PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'blocked')`)
	endpoint := newStalledEndpoint(t)
	attempted := make(chan pq.LSN, 1)
	run := startExistingConstructorKafka(t, source.config(t, func(cfg *kafkaconfig.Connector) {
		cfg.Kafka.Brokers = []string{endpoint.addr()}
		cfg.Kafka.WriteTimeout = time.Minute
		cfg.Kafka.ReadTimeout = time.Minute
		cfg.Kafka.Online.MaxAttempts = 1
	}), func(message *kafkacdc.Message) []gokafka.Message {
		attempted <- message.LSN
		return transformExistingHandler(message)
	})
	run.waitReady(t)
	var high pq.LSN
	select {
	case high = <-attempted:
	case <-time.After(30 * time.Second):
		t.Fatal("Handler never observed READ")
	}
	endpoint.waitAccepted(t)
	ceiling := source.job(t).SourceLSN
	numeric, err := pq.ParseLSN(ceiling)
	require.NoError(t, err)
	require.Less(t, numeric, high, "blocked READ boundary is strictly later than durable checkpoint")
	source.assertFrontierCeilings(t, ceiling, high.String(), "existing Handler write actively blocked")
	run.stop(t)
	require.GreaterOrEqual(t, run.connector.Stats().Delivery.Unknown, uint64(1))
	source.assertFrontierCeilings(t, ceiling, high.String(), "existing Handler after cancellation")
	require.False(t, run.connector.Health().Running)
	t.Logf("blocked READ HIGH=%s, ordinal=%s; cancellation never certified durable completion", high, strconv.Itoa(0))
}
