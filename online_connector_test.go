package cdc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	kafkapkg "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// validOnlineConfig is an admitted online configuration: strategy online,
// explicit publication with FULL images, protocol 1, acks=all and explicit
// destinations. Construction opens no connection, so no database is needed.
func validOnlineConfig() config.Connector {
	return config.Connector{
		CDC: cdcconfig.Config{
			Host:     "127.0.0.1",
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				Name:              "online_pub",
				CreateIfNotExists: true,
				Operations:        publication.OperationOptions,
				Tables: publication.Tables{{
					Schema:          "public",
					Name:            "accounts",
					ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{Name: "online_slot", ProtoVersion: 1, CreateIfNotExists: true},
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled:   true,
				Mode:      cdcconfig.SnapshotModeInitial,
				Strategy:  cdcconfig.SnapshotStrategyOnline,
				ChunkSize: 2,
			},
		},
		Kafka: config.Kafka{
			Brokers:           []string{"127.0.0.1:9092"},
			RequiredAcks:      -1,
			TableTopicMapping: map[string]string{"public.accounts": "accounts.topic"},
		},
	}
}

func TestNewOnlineConnectorAdmissionMatrix(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(cfg *config.Connector)
	}{
		{"consistent strategy", func(cfg *config.Connector) { cfg.CDC.Snapshot.Strategy = cdcconfig.SnapshotStrategyConsistent }},
		{"snapshot only mode", func(cfg *config.Connector) { cfg.CDC.Snapshot.Mode = cdcconfig.SnapshotModeSnapshotOnly }},
		{"leader-only acks", func(cfg *config.Connector) { cfg.Kafka.RequiredAcks = 1 }},
		{"acks all-replicas is not a leader ack", func(cfg *config.Connector) { cfg.Kafka.RequiredAcks = 2 }},
		{"skip oversized", func(cfg *config.Connector) { cfg.Kafka.SkipOversizedMessages = true }},
		{"auto topic creation", func(cfg *config.Connector) { cfg.Kafka.AllowAutoTopicCreation = true }},
		{"round robin balancer", func(cfg *config.Connector) { cfg.Kafka.Balancer = "RoundRobin" }},
		{"least bytes balancer", func(cfg *config.Connector) { cfg.Kafka.Balancer = "LeastBytes" }},
		{"unknown balancer", func(cfg *config.Connector) { cfg.Kafka.Balancer = "Sticky" }},
		{"no mapping", func(cfg *config.Connector) { cfg.Kafka.TableTopicMapping = nil }},
		{"unmapped table", func(cfg *config.Connector) {
			cfg.Kafka.TableTopicMapping = map[string]string{"public.other": "other.topic"}
		}},
		{"empty topic", func(cfg *config.Connector) {
			cfg.Kafka.TableTopicMapping = map[string]string{"public.accounts": " "}
		}},
		{"unbounded attempts", func(cfg *config.Connector) { cfg.Kafka.Online.MaxAttempts = 11 }},
		{"zero attempts", func(cfg *config.Connector) { cfg.Kafka.Online.MaxAttempts = -1 }},
		{"negative close timeout", func(cfg *config.Connector) { cfg.Kafka.Online.CloseTimeout = -time.Second }},
		{"negative write timeout", func(cfg *config.Connector) { cfg.Kafka.WriteTimeout = -time.Second }},
		{"negative read timeout", func(cfg *config.Connector) { cfg.Kafka.ReadTimeout = -time.Second }},
		{"unknown compression", func(cfg *config.Connector) { cfg.Kafka.Compression = 5 }},
		{"no brokers", func(cfg *config.Connector) { cfg.Kafka.Brokers = nil }},
		{"empty broker", func(cfg *config.Connector) { cfg.Kafka.Brokers = []string{" "} }},
		{"empty max message bytes", func(cfg *config.Connector) { cfg.Kafka.MaxMessageBytes = "0b" }},
		{"invalid max message bytes", func(cfg *config.Connector) { cfg.Kafka.MaxMessageBytes = "one megabyte" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validOnlineConfig()
			test.mutate(&cfg)
			connector, err := NewOnlineConnector(t.Context(), cfg)
			require.Error(t, err)
			require.Nil(t, connector)
			require.True(t,
				errors.Is(err, ErrOnlineAdmission) || errors.Is(err, errCoreOnlineConfig),
				"admission must fail closed through a typed error, got %v", err)
		})
	}
}

// errCoreOnlineConfig stands for the core configuration errors that are also
// reported during construction for an unusable snapshot/slot profile.
var errCoreOnlineConfig = errors.New("unsupported")

func TestNewOnlineConnectorAcceptsAndOwnsItsConfiguration(t *testing.T) {
	cfg := validOnlineConfig()
	connector, err := NewOnlineConnector(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(connector.Close)

	owned, ok := connector.(*onlineConnector)
	require.True(t, ok)

	// Online defaults are applied without touching the legacy defaults.
	require.Equal(t, -1, owned.cfg.Kafka.RequiredAcks)
	require.Equal(t, 3, owned.cfg.Kafka.Online.MaxAttempts)
	require.Equal(t, 1<<20, int(owned.delivery.maxBytes))
	require.Equal(t, "accounts.topic", owned.delivery.topics["public.accounts"])

	// Later caller mutations must not reach the connector.
	cfg.Kafka.Brokers[0] = "mutated:1"
	cfg.Kafka.TableTopicMapping["public.accounts"] = "mutated.topic"
	cfg.CDC.Publication.Tables[0].Name = "mutated"
	require.Equal(t, "127.0.0.1:9092", owned.cfg.Kafka.Brokers[0])
	require.Equal(t, "accounts.topic", owned.delivery.topics["public.accounts"])
	require.Equal(t, "accounts", owned.cfg.CDC.Publication.Tables[0].Name)

	// No connection was opened: health is "new" and nothing is running.
	health := connector.Health()
	require.Equal(t, OnlineStateNew, health.State)
	require.False(t, health.Running)
	require.Empty(t, health.Err)
	require.Equal(t, OnlineStats{}, connector.Stats())
	select {
	case <-connector.Done():
		t.Fatal("an unstarted connector must not report Done")
	default:
	}
}

func TestNewOnlineConnectorRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	connector, err := NewOnlineConnector(ctx, validOnlineConfig())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, connector)
}

func TestOnlineConnectorCloseBeforeStartIsNotReady(t *testing.T) {
	connector, err := NewOnlineConnector(t.Context(), validOnlineConfig())
	require.NoError(t, err)

	connector.Close()

	select {
	case <-connector.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Close before Start must finish Done")
	}
	require.ErrorIs(t, connector.Err(), context.Canceled)
	require.ErrorIs(t, connector.WaitUntilReady(t.Context()), ErrOnlineConnectorClosed)
	require.ErrorIs(t, connector.WaitUntilSnapshotComplete(t.Context()), ErrOnlineConnectorClosed)

	health := connector.Health()
	require.False(t, health.Running, "a closed connector must never report running")
	require.Equal(t, OnlineStateClosed, health.State)

	// A later Start cannot resurrect it.
	connector.Start(t.Context())
	require.False(t, connector.Health().Running)
	require.ErrorIs(t, connector.WaitUntilReady(t.Context()), ErrOnlineConnectorClosed)

	// Close is idempotent.
	connector.Close()
}

// TestNewOnlineConnectorDefaultsAcksToAll proves the online path defaults to
// acks=all while the legacy path keeps its leader-ack default of 1.
func TestNewOnlineConnectorDefaultsAcksToAll(t *testing.T) {
	cfg := validOnlineConfig()
	cfg.Kafka.RequiredAcks = 0

	connector, err := NewOnlineConnector(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(connector.Close)

	owned := connector.(*onlineConnector)
	require.Equal(t, -1, owned.cfg.Kafka.RequiredAcks)
}

// stalledBroker accepts TCP connections and never answers, which is the shape of
// a broker that stalled after metadata discovery. It is a real socket, so the
// writer under test is the production one.
type stalledBroker struct {
	listener net.Listener
	accepted chan struct{}
	done     chan struct{}
	mu       sync.Mutex
	conns    []net.Conn
	once     sync.Once
}

func newStalledBroker(t *testing.T) *stalledBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	broker := &stalledBroker{listener: listener, accepted: make(chan struct{}), done: make(chan struct{})}
	go broker.serve()
	t.Cleanup(broker.close)
	return broker
}

func (b *stalledBroker) serve() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns = append(b.conns, conn)
		b.mu.Unlock()
		b.once.Do(func() { close(b.accepted) })
	}
}

func (b *stalledBroker) addr() string { return b.listener.Addr().String() }

func (b *stalledBroker) waitAccepted(t *testing.T) {
	t.Helper()
	select {
	case <-b.accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the writer never opened a connection to the stalled broker")
	}
}

func (b *stalledBroker) close() {
	close(b.done)
	_ = b.listener.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, conn := range b.conns {
		_ = conn.Close()
	}
}

// stubOnlineCore is a double for the core lifecycle ONLY; every broker
// acknowledgement in these tests still comes from the real Kafka writer.
type stubOnlineCore struct {
	done        chan struct{}
	closeDelay  time.Duration
	closeCalls  atomic.Int32
	err         error
	startedOnce sync.Once
}

func newStubOnlineCore() *stubOnlineCore {
	return &stubOnlineCore{done: make(chan struct{})}
}

func (s *stubOnlineCore) Start(ctx context.Context) {
	<-ctx.Done()
	s.startedOnce.Do(func() { close(s.done) })
}

func (s *stubOnlineCore) WaitUntilReady(context.Context) error            { return nil }
func (s *stubOnlineCore) WaitUntilCaptureReady(context.Context) error     { return nil }
func (s *stubOnlineCore) WaitUntilSnapshotComplete(context.Context) error { return nil }
func (s *stubOnlineCore) SetMetricCollectors(...prometheus.Collector)     {}
func (s *stubOnlineCore) Stats() cdc.OnlineStats                          { return cdc.OnlineStats{} }
func (s *stubOnlineCore) Health() cdc.OnlineHealth                        { return cdc.OnlineHealth{} }
func (s *stubOnlineCore) GetConfig() *cdcconfig.Config                    { return &cdcconfig.Config{} }
func (s *stubOnlineCore) Done() <-chan struct{}                           { return s.done }
func (s *stubOnlineCore) Err() error                                      { return s.err }

func (s *stubOnlineCore) Close() {
	s.closeCalls.Add(1)
	if s.closeDelay > 0 {
		time.Sleep(s.closeDelay)
	}
}

// newBoundedConnector assembles a connector around a real writer pointed at the
// given brokers, so shutdown bounds are measured against production I/O paths.
func newBoundedConnector(t *testing.T, brokers []string, closeTimeout time.Duration, core cdc.OnlineConnectorLifecycle) *onlineConnector {
	t.Helper()
	cfg := &config.Connector{Kafka: config.Kafka{
		Brokers:      brokers,
		RequiredAcks: -1,
		Balancer:     "Hash",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
		Online:       config.OnlineKafka{MaxAttempts: 1, CloseTimeout: closeTimeout},
	}}
	// Mirror the constructor: connector logging must be safe before core Start.
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	lifetime, lifetimeAbort := context.WithCancel(context.Background())
	writer, err := kafkapkg.NewOnlineWriter(cfg, kafkapkg.OnlineWriterOptions{Lifetime: lifetime, BatchBytes: 1 << 20, MaxAttempts: 1})
	require.NoError(t, err)
	delivery := newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20)
	return &onlineConnector{
		core:          core,
		delivery:      delivery,
		cfg:           cfg,
		lifetime:      lifetime,
		lifetimeAbort: lifetimeAbort,
		closeTimeout:  closeTimeout,
		done:          make(chan struct{}),
		cleanupDone:   make(chan struct{}),
		state:         OnlineStateNew,
	}
}

// TestOnlineConnectorCloseBoundsStalledBroker proves one overall shutdown budget
// bounds a stalled in-flight Produce instead of waiting for the writer timeout.
func TestOnlineConnectorCloseBoundsStalledBroker(t *testing.T) {
	broker := newStalledBroker(t)
	core := newStubOnlineCore()
	connector := newBoundedConnector(t, []string{broker.addr()}, 1500*time.Millisecond, core)

	connector.Start(context.Background())
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- connector.delivery.Handle(context.Background(), onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1}))
	}()
	broker.waitAccepted(t)

	started := time.Now()
	connector.Close()
	elapsed := time.Since(started)
	require.Less(t, elapsed, 3*time.Second, "Close must bound stalled broker I/O, took %s", elapsed)

	select {
	case err := <-writeDone:
		require.Error(t, err, "an aborted write can never be reported as delivered")
		require.Equal(t, uint64(0), connector.Stats().Delivery.Written)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight delivery did not return after Close")
	}

	select {
	case <-connector.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not finish Done")
	}
	require.NoError(t, connector.Err(), "cleanup finished inside the budget")
	require.Equal(t, OnlineStateClosed, connector.Health().State, "a completed cleanup is the only path to Closed")
	require.False(t, connector.Health().Running)
	require.EqualValues(t, 1, core.closeCalls.Load())
}

// TestOnlineConnectorCloseReportsIncompleteCleanup proves an overrunning cleanup
// is reported instead of being presented as a clean close, while the cleanup
// itself still completes and stays observable.
func TestOnlineConnectorCloseReportsIncompleteCleanup(t *testing.T) {
	core := newStubOnlineCore()
	core.closeDelay = 2 * time.Second
	connector := newBoundedConnector(t, []string{"127.0.0.1:1"}, 200*time.Millisecond, core)
	connector.Start(context.Background())

	started := time.Now()
	connector.Close()
	require.Less(t, time.Since(started), time.Second, "Close must return within its budget")

	require.ErrorIs(t, connector.Err(), ErrOnlineShutdownIncomplete)
	require.Contains(t, connector.Health().Err, ErrOnlineShutdownIncomplete.Error())
	// An unresolved cleanup must never look like a clean close.
	require.Equal(t, OnlineStateFailed, connector.Health().State)
	require.False(t, connector.Health().Running)

	// The cleanup keeps running and completes; the incomplete report is retained
	// and the state is still not a clean Closed.
	select {
	case <-connector.cleanupDone:
	case <-time.After(10 * time.Second):
		t.Fatal("background cleanup never finished")
	}
	require.ErrorIs(t, connector.Err(), ErrOnlineShutdownIncomplete)
	require.Equal(t, OnlineStateFailed, connector.Health().State)
	require.EqualValues(t, 1, core.closeCalls.Load())
}

// TestOnlineConnectorCloseIsConcurrent proves concurrent Close calls share one
// cleanup and all return inside the budget.
func TestOnlineConnectorCloseIsConcurrent(t *testing.T) {
	core := newStubOnlineCore()
	connector := newBoundedConnector(t, []string{"127.0.0.1:1"}, 2*time.Second, core)
	connector.Start(context.Background())

	var waitGroup sync.WaitGroup
	for i := 0; i < 4; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			connector.Close()
		}()
	}
	finished := make(chan struct{})
	go func() { waitGroup.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Close calls did not finish")
	}
	select {
	case <-connector.cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish")
	}
	require.EqualValues(t, 1, core.closeCalls.Load(), "cleanup must run exactly once")
}

// TestCloneOnlineConfigOwnsHeartbeatColumns proves the owned copy includes the
// heartbeat table column slice, not just the table name.
func TestCloneOnlineConfigOwnsHeartbeatColumns(t *testing.T) {
	cfg := validOnlineConfig()
	cfg.CDC.Heartbeat.Table.Columns = []string{"id", "touched_at"}

	owned := cloneOnlineConfig(cfg)
	cfg.CDC.Heartbeat.Table.Columns[0] = "mutated"
	require.Equal(t, []string{"id", "touched_at"}, owned.CDC.Heartbeat.Table.Columns)
}

// newErrorConnector assembles a connector around a controlled writer so terminal
// error identity and cleanup state can be asserted without any broker I/O.
func newErrorConnector(t *testing.T, core cdc.OnlineConnectorLifecycle, writer *controlledWriter, closeTimeout time.Duration) *onlineConnector {
	t.Helper()
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	return &onlineConnector{
		core:          core,
		delivery:      newTestDelivery(writer, map[string]string{"public.accounts": "accounts.topic"}, 1<<20),
		cfg:           &config.Connector{},
		lifetime:      context.Background(),
		lifetimeAbort: func() {},
		closeTimeout:  closeTimeout,
		done:          make(chan struct{}),
		cleanupDone:   make(chan struct{}),
		state:         OnlineStateNew,
	}
}

// TestOnlineConnectorErrJoinsCoreAndShutdownCauses proves the terminal error
// keeps BOTH the original capture/broker cause and the cleanup timeout, before
// and after the delayed cleanup eventually finishes.
func TestOnlineConnectorErrJoinsCoreAndShutdownCauses(t *testing.T) {
	coreCause := errors.New("capture stopped: broker rejected the stream")
	core := newStubOnlineCore()
	core.err = coreCause
	core.closeDelay = 1500 * time.Millisecond

	connector := newErrorConnector(t, core, newControlledWriter(), 200*time.Millisecond)
	connector.Start(context.Background())

	started := time.Now()
	connector.Close()
	require.Less(t, time.Since(started), time.Second)

	err := connector.Err()
	require.ErrorIs(t, err, coreCause, "the original capture/broker cause must survive")
	require.ErrorIs(t, err, ErrOnlineShutdownIncomplete, "the cleanup timeout must be reported too")

	select {
	case <-connector.cleanupDone:
	case <-time.After(10 * time.Second):
		t.Fatal("background cleanup never finished")
	}
	after := connector.Err()
	require.ErrorIs(t, after, coreCause, "eventual cleanup must not clear the original cause")
	require.ErrorIs(t, after, ErrOnlineShutdownIncomplete, "eventual cleanup must not clear the timeout")
	require.Contains(t, connector.Health().Err, ErrOnlineShutdownIncomplete.Error())
}

// TestOnlineConnectorCleanupErrorMarksFailed proves a completed cleanup that hit
// a writer-close error reports Failed (never Closing, never a clean Closed) and
// keeps both causes in the chain.
func TestOnlineConnectorCleanupErrorMarksFailed(t *testing.T) {
	coreCause := errors.New("capture stopped: durable metadata refused")
	writerCause := errors.New("writer close failed")
	core := newStubOnlineCore()
	core.err = coreCause
	writer := newControlledWriter()
	writer.closeErr = writerCause

	connector := newErrorConnector(t, core, writer, 5*time.Second)
	connector.Start(context.Background())

	connector.Close()
	select {
	case <-connector.cleanupDone:
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup did not finish")
	}

	select {
	case <-connector.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not finish Done")
	}
	require.Equal(t, OnlineStateFailed, connector.Health().State, "a cleanup error must not look clean")
	require.ErrorIs(t, connector.Err(), writerCause)
	require.ErrorIs(t, connector.Err(), coreCause)
	require.EqualValues(t, 1, core.closeCalls.Load())
}
