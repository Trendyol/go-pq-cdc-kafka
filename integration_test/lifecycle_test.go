package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_WaitUntilReadyAfterClose(t *testing.T) {
	ctx := context.Background()
	connector := newLifecycleConnector(ctx, t, "cdc_slot_lifecycle_closed", "cdc_publication_lifecycle_closed", "users_lifecycle_closed")

	go connector.Start(ctx)

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	connector.Close()

	afterCloseCtx, afterCloseCancel := context.WithTimeout(ctx, 2*time.Second)
	defer afterCloseCancel()

	err := connector.WaitUntilReady(afterCloseCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector closed")
}

func TestConnector_CloseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	connector := newLifecycleConnector(ctx, t, "cdc_slot_lifecycle_idempotent", "cdc_publication_lifecycle_idempotent", "users_lifecycle_idempotent")

	go connector.Start(ctx)

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	require.NotPanics(t, func() {
		connector.Close()
		connector.Close()
	})
}

func TestConnector_CloseBeforeWaitUntilReadyDoesNotHang(t *testing.T) {
	ctx := context.Background()
	connector := newLifecycleConnector(ctx, t, "cdc_slot_lifecycle_early_close", "cdc_publication_lifecycle_early_close", "users_lifecycle_early_close")

	go connector.Start(ctx)

	waitForConnectorReady(ctx, t, connector)
	connector.Close()

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	started := time.Now()
	err := connector.WaitUntilReady(waitCtx)
	elapsed := time.Since(started)

	assert.Less(t, elapsed, 3500*time.Millisecond, "WaitUntilReady must not hang after Close")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector closed")
}

func newLifecycleConnector(ctx context.Context, t *testing.T, slotName, publicationName, tableName string) cdc.Connector {
	t.Helper()

	db, err := sql.Open("postgres", fmt.Sprintf(
		"postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable",
		Infra.PostgresHost, Infra.PostgresPort,
	))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)

	postgresPort, err := strconv.Atoi(Infra.PostgresPort)
	require.NoError(t, err)

	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              publicationName,
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            tableName,
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        slotName,
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping: map[string]string{
				fmt.Sprintf("public.%s", tableName): fmt.Sprintf("%s.test", tableName),
			},
			Brokers:                     []string{fmt.Sprintf("%s:%s", Infra.KafkaHost, Infra.KafkaPort)},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 100,
			ProducerBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, func(_ *cdc.Message) []kafka.Message {
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(connector.Close)

	return connector
}
