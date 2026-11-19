package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

// ErrOnlineConnectorClosed reports readiness requested after Close. A closed
// connector is never ready, even when its capture had reached readiness before.
var ErrOnlineConnectorClosed = errors.New("online kafka connector is closed")

// ErrOnlineShutdownIncomplete reports that Close returned before the owned
// capture/writer cleanup finished inside online.closeTimeout. The cleanup keeps
// running in the background, the connector never claims a clean close, and no
// acknowledgement or delivery is implied by it.
var ErrOnlineShutdownIncomplete = errors.New("online kafka shutdown incomplete")

// OnlineState is the observable lifecycle state of an OnlineConnector.
type OnlineState string

const (
	OnlineStateNew     OnlineState = "new"
	OnlineStateRunning OnlineState = "running"
	OnlineStateClosing OnlineState = "closing"
	OnlineStateClosed  OnlineState = "closed"
	OnlineStateFailed  OnlineState = "failed"
)

// OnlineHealth is a bounded point-in-time view. Running never reports true after
// Close, and a startup or delivery failure is visible as State/Err rather than
// as a silent stop.
type OnlineHealth struct {
	State   OnlineState
	Err     string
	Running bool
}

// OnlineStats combines the core capture/metadata counters with the delivery
// counters. Bytes reported by the core are retained encoded work, not heap.
type OnlineStats struct {
	Core     cdc.OnlineStats
	Delivery OnlineDeliveryStats
}

// OnlineConnector is the additive, strategy-online Kafka connector. Start drives
// core online capture; the delivery path writes each transformed output and returns
// only after the broker acknowledged every output with acks=all. The core coordinator
// remains the single owner of the replication acknowledgement.
type OnlineConnector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	WaitUntilSnapshotComplete(ctx context.Context) error
	Close()
	Done() <-chan struct{}
	Err() error
	Stats() OnlineStats
	Health() OnlineHealth
}

type onlineConnector struct {
	run           context.Context
	lifetime      context.Context
	cancel        context.CancelFunc
	lifetimeAbort context.CancelFunc
	core          cdc.OnlineConnectorLifecycle
	delivery      *onlineDelivery
	cfg           *config.Connector
	done          chan struct{}
	cleanupDone   chan struct{}
	state         OnlineState
	shutdownErr   error
	closeTimeout  time.Duration
	started       bool
	closed        bool
	mu            sync.Mutex
	doneOnce      sync.Once
	closeOnce     sync.Once
}

// NewOnlineConnector builds the additive online connector: core online capture
// (primary-only, protocol 1, FULL images, immutable integer key) composed with
// an owned synchronous Kafka writer. Construction opens no connection, so an
// unreachable broker surfaces through Done/Err and readiness, not here.
func NewOnlineConnector(ctx context.Context, cfg config.Connector, options ...OnlineOption) (OnlineConnector, error) {
	return newOnlineConnector(ctx, cfg, nil, options...)
}

func newOnlineConnector(ctx context.Context, cfg config.Connector, handler Handler, options ...OnlineOption) (*onlineConnector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owned := cloneOnlineConfig(cfg)
	if owned.Kafka.RequiredAcks == 0 {
		// acks=all is the online default; the legacy default stays 1.
		owned.Kafka.RequiredAcks = requireAllAcks
	}
	owned.CDC.SetDefault()
	owned.SetDefault()

	admission, err := newOnlineAdmission(&owned)
	if err != nil {
		return nil, err
	}
	settings, err := applyOnlineOptions(options)
	if err != nil {
		return nil, err
	}
	if settings.logger != nil {
		owned.CDC.Logger.Logger = settings.logger
	}

	// Core's logger package panics until InitLogger ran, and core only runs it in
	// Start. Initializing the configured logger here keeps Close/error logging
	// safe before Start; InitLogger is once-guarded, so core's own call cannot
	// change the logger later.
	logger.InitLogger(owned.CDC.Logger.Logger)

	// One lifetime governs the owned broker I/O: kafka-go writes use their own
	// background context, so Close aborts through this seam.
	lifetime, lifetimeAbort := context.WithCancel(context.Background())

	delivery, err := newOnlineDelivery(lifetime, &owned, admission)
	if err != nil {
		lifetimeAbort()
		return nil, err
	}

	delivery.handler = handler

	coreOptions := make([]cdc.OnlineConnectorOption, 0, 1)
	if settings.observer != nil {
		coreOptions = append(coreOptions, cdc.WithOnlineObserver(settings.observer))
	}
	core, err := cdc.NewConnectorWithOptions(ctx, owned.CDC, cdc.WithOnlineHandler(delivery.Handle, coreOptions...))
	if err != nil {
		lifetimeAbort()
		if closeErr := delivery.close(); closeErr != nil {
			logger.Error("online kafka writer close after construction failure", "error", closeErr)
		}
		return nil, fmt.Errorf("online core connector: %w", err)
	}
	lifecycle, ok := core.(cdc.OnlineConnectorLifecycle)
	if !ok {
		core.Close()
		lifetimeAbort()
		if closeErr := delivery.close(); closeErr != nil {
			logger.Error("online kafka writer close after construction failure", "error", closeErr)
		}
		return nil, errors.New("online core connector does not expose the online lifecycle")
	}

	return &onlineConnector{
		core:          lifecycle,
		delivery:      delivery,
		cfg:           &owned,
		lifetime:      lifetime,
		lifetimeAbort: lifetimeAbort,
		closeTimeout:  admission.closeTimeout,
		done:          make(chan struct{}),
		cleanupDone:   make(chan struct{}),
		state:         OnlineStateNew,
	}, nil
}

// applyOnlineOptions collects the typed construction options, refusing the first
// option that cannot be applied.
func applyOnlineOptions(options []OnlineOption) (onlineOptions, error) {
	settings := onlineOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&settings); err != nil {
			return onlineOptions{}, err
		}
	}
	return settings, nil
}

// newOnlineDelivery owns the synchronous writer and the routing table, so the
// constructor stays a sequence of admission and composition steps.
func newOnlineDelivery(lifetime context.Context, cfg *config.Connector, admission onlineAdmission) (*onlineDelivery, error) {
	writer, err := kafka.NewOnlineWriter(cfg, kafka.OnlineWriterOptions{
		Lifetime:    lifetime,
		BatchBytes:  admission.maxBytes,
		MaxAttempts: admission.maxAttempts,
	})
	if err != nil {
		return nil, err
	}
	return &onlineDelivery{
		writer:         writer,
		topics:         admission.topics,
		maxBytes:       admission.maxBytes,
		maxRecords:     admission.maxRecords,
		maxOutputBytes: admission.maxOutputBytes,
		counters:       &onlineDeliveryCounters{},
	}, nil
}

// Start runs core capture in a goroutine. Only the first call does work; a call
// after Close is ignored so a closed connector can never become ready.
func (c *onlineConnector) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started || c.closed {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.state = OnlineStateRunning
	// The returned cancel is owned by this connector and invoked by Close; it
	// never cancels the caller's context.
	c.run, c.cancel = context.WithCancel(ctx) //nolint:gosec // cancel is stored and called by Close
	run := c.run
	c.mu.Unlock()

	go func() {
		c.core.Start(run)
		c.finish()
	}()
}

func (c *onlineConnector) finish() {
	terminal := c.core.Err()
	c.mu.Lock()
	// Capture termination may only resolve a RUNNING connector; a pending or
	// incomplete shutdown state is owned by shutdown/Close and must stay visible.
	if c.state == OnlineStateRunning {
		c.state = OnlineStateClosed
		if terminal != nil && !errors.Is(terminal, context.Canceled) {
			c.state = OnlineStateFailed
		}
	}
	c.mu.Unlock()
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *onlineConnector) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrOnlineConnectorClosed
	}
	return nil
}

// WaitUntilReady waits for core capture readiness. It never reports ready after
// Close, and a capture failure surfaces as the core error.
func (c *onlineConnector) WaitUntilReady(ctx context.Context) error {
	if err := c.closedError(); err != nil {
		return err
	}
	if err := c.core.WaitUntilReady(ctx); err != nil {
		return err
	}
	return c.closedError()
}

// WaitUntilSnapshotComplete waits for the online READ phase to be delivered.
func (c *onlineConnector) WaitUntilSnapshotComplete(ctx context.Context) error {
	if err := c.closedError(); err != nil {
		return err
	}
	if err := c.core.WaitUntilSnapshotComplete(ctx); err != nil {
		return err
	}
	return c.closedError()
}

// Close cancels only the context this connector derived, aborts its owned broker
// I/O and then releases capture and writer inside ONE overall budget. It is
// idempotent and safe before Start, where it finishes readiness/Done without
// opening anything.
//
// kafka-go starts Produce with its own background context, so caller
// cancellation cannot stop a stalled request; aborting the writer lifetime is
// what keeps both the core handler and Writer.Close bounded. A step that still
// overruns the budget is reported through ErrOnlineShutdownIncomplete and keeps
// running in the background instead of being reported as a clean close.
func (c *onlineConnector) Close() {
	c.closeOnce.Do(func() {
		deadline := time.Now().Add(c.closeTimeout)

		c.mu.Lock()
		c.closed = true
		started := c.started
		if c.state != OnlineStateFailed {
			c.state = OnlineStateClosing
		}
		cancel := c.cancel
		c.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		// Abort owned broker I/O before asking core to stop so a stalled write
		// cannot keep the core handler (and therefore core.Close) waiting.
		c.lifetimeAbort()

		go c.shutdown(started, time.Until(deadline))

		select {
		case <-c.cleanupDone:
		case <-time.After(time.Until(deadline)):
			err := fmt.Errorf("%w: capture/writer cleanup still running after %s", ErrOnlineShutdownIncomplete, c.closeTimeout)
			c.setShutdownErr(err)
			c.mu.Lock()
			// An unresolved cleanup is never reported as a clean close.
			c.state = OnlineStateFailed
			c.mu.Unlock()
			logger.Error("online connector shutdown exceeded its budget", "timeout", c.closeTimeout, "error", err)
		}
	})
}

// shutdown performs the owned cleanup exactly once and always closes
// cleanupDone, so an overrunning step stays observable instead of being lost.
func (c *onlineConnector) shutdown(started bool, budget time.Duration) {
	defer close(c.cleanupDone)

	c.core.Close()
	if started {
		<-c.core.Done()
	} else {
		c.doneOnce.Do(func() { close(c.done) })
	}
	_ = budget // the writer bounds its own socket drain; the caller budget governs the report
	if err := c.delivery.close(); err != nil {
		c.setShutdownErr(fmt.Errorf("online kafka writer close: %w", err))
		logger.Error("online kafka writer close", "error", err)
	}

	c.mu.Lock()
	switch {
	case c.shutdownErr != nil:
		// Cleanup finished but something failed (writer close, socket teardown):
		// never leave the connector looking like it is still closing or clean.
		c.state = OnlineStateFailed
	case c.state == OnlineStateClosing:
		c.state = OnlineStateClosed
	}
	c.mu.Unlock()
}

func (c *onlineConnector) setShutdownErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdownErr = errors.Join(c.shutdownErr, err)
}

func (c *onlineConnector) Done() <-chan struct{} { return c.done }

// Err reports why capture stopped: nil while running, context.Canceled after a
// caller-initiated Close, the core error after a failure, and
// ErrOnlineShutdownIncomplete when Close returned before its cleanup finished.
func (c *onlineConnector) Err() error {
	c.mu.Lock()
	shutdownErr := c.shutdownErr
	c.mu.Unlock()
	// Both causes stay in the chain: errors.Is keeps working for the original
	// capture/broker cause AND for the cleanup timeout/writer-close failure.
	return errors.Join(c.core.Err(), shutdownErr)
}

func (c *onlineConnector) Stats() OnlineStats {
	return OnlineStats{Core: c.core.Stats(), Delivery: c.delivery.snapshot()}
}

func (c *onlineConnector) Health() OnlineHealth {
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	health := OnlineHealth{State: state}
	if err := c.Err(); err != nil {
		health.Err = err.Error()
	}
	select {
	case <-c.core.Done():
	default:
		health.Running = state == OnlineStateRunning
	}
	return health
}

// cloneOnlineConfig owns every mutable slice, map and pointer the connector can
// observe, so later caller mutations (or core defaults) cannot leak either way.
func cloneOnlineConfig(cfg config.Connector) config.Connector {
	owned := cfg
	owned.Kafka.Brokers = append([]string(nil), cfg.Kafka.Brokers...)
	owned.Kafka.MetadataTopics = append([]string(nil), cfg.Kafka.MetadataTopics...)
	owned.Kafka.RootCA = append([]byte(nil), cfg.Kafka.RootCA...)
	owned.Kafka.InterCA = append([]byte(nil), cfg.Kafka.InterCA...)
	owned.Kafka.TableTopicMapping = make(map[string]string, len(cfg.Kafka.TableTopicMapping))
	for table, topic := range cfg.Kafka.TableTopicMapping {
		owned.Kafka.TableTopicMapping[table] = topic
	}

	owned.CDC.Publication.Tables = cloneOnlineTables(cfg.CDC.Publication.Tables)
	owned.CDC.Snapshot.Tables = cloneOnlineTables(cfg.CDC.Snapshot.Tables)
	owned.CDC.Heartbeat.Table.Columns = append([]string(nil), cfg.CDC.Heartbeat.Table.Columns...)
	owned.CDC.VisibilityGuard.Replicas = append([]string(nil), cfg.CDC.VisibilityGuard.Replicas...)
	owned.CDC.Publication.Operations = append(publication.Operations(nil), cfg.CDC.Publication.Operations...)
	if cfg.CDC.StatusUpdateInterval != nil {
		interval := *cfg.CDC.StatusUpdateInterval
		owned.CDC.StatusUpdateInterval = &interval
	}
	return owned
}

func cloneOnlineTables(tables publication.Tables) publication.Tables {
	owned := append(publication.Tables(nil), tables...)
	for i := range owned {
		owned[i].Columns = append([]string(nil), tables[i].Columns...)
	}
	return owned
}
