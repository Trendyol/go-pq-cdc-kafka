package kafka

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func onlineWriterConfig() *config.Connector {
	return &config.Connector{Kafka: config.Kafka{
		Brokers:      []string{"127.0.0.1:9092"},
		RequiredAcks: -1,
		Balancer:     "Hash",
		WriteTimeout: 0,
		ReadTimeout:  0,
	}}
}

// TestNewOnlineWriterEnforcesAcksAll checks the actual writer configuration, not
// just the constructor argument: an acknowledged write needs RequireAll.
func TestNewOnlineWriterEnforcesAcksAll(t *testing.T) {
	cfg := onlineWriterConfig()
	cfg.Kafka.RequiredAcks = 1
	writer, err := NewOnlineWriter(cfg, OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 3})
	require.Error(t, err)
	require.Nil(t, writer)

	cfg.Kafka.RequiredAcks = -1
	writer, err = NewOnlineWriter(cfg, OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 3})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	require.Equal(t, gokafka.RequireAll, writer.writer.RequiredAcks)
	require.False(t, writer.writer.Async, "an asynchronous writer reports success before the broker acked")
	require.Equal(t, 1, writer.writer.BatchSize, "one event per request keeps the acknowledgement per event")
	require.Equal(t, 3, writer.writer.MaxAttempts)
	require.EqualValues(t, 1<<20, writer.writer.BatchBytes)
	require.False(t, writer.writer.AllowAutoTopicCreation, "missing topics must fail instead of being invented")
	// The writer reaches the broker only through the lifetime-aware wrapper around
	// the transport this connector owns.
	wrapped, ok := writer.writer.Transport.(*onlineRoundTripper)
	require.True(t, ok)
	require.Same(t, writer.transport, wrapped.inner, "the writer uses the transport this connector owns")
}

func TestNewOnlineWriterRejectsUnboundedOrEmptyOptions(t *testing.T) {
	tests := []struct {
		name    string
		options OnlineWriterOptions
	}{
		{"no attempts", OnlineWriterOptions{BatchBytes: 1 << 20}},
		{"no batch bytes", OnlineWriterOptions{MaxAttempts: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, err := NewOnlineWriter(onlineWriterConfig(), test.options)
			require.Error(t, err)
			require.Nil(t, writer)
		})
	}
}

// TestOnlineWriterRefusesWritesAfterClose proves a released writer cannot report
// a successful write, so a closed connector can never certify delivery.
func TestOnlineWriterRefusesWritesAfterClose(t *testing.T) {
	writer, err := NewOnlineWriter(onlineWriterConfig(), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	err = writer.WriteMessages(context.Background(), gokafka.Message{Topic: "t", Value: []byte("v")})
	require.Error(t, err)
}

// stalledTransport models a broker that accepted the connection but never
// answers: every round trip blocks until its context is cancelled, which is the
// shape kafka-go's background Produce would otherwise keep waiting on.
type stalledTransport struct {
	entered   chan struct{}
	mu        sync.Mutex
	closeIdle int
}

func newStalledTransport() *stalledTransport {
	return &stalledTransport{entered: make(chan struct{}, 8)}
}

func (t *stalledTransport) RoundTrip(ctx context.Context, _ net.Addr, _ gokafka.Request) (gokafka.Response, error) {
	select {
	case t.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *stalledTransport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeIdle++
}

func (t *stalledTransport) closeIdleCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeIdle
}

// slowWriteConfig keeps the writer-level timeout far larger than the test bound,
// so only the abort seam (not the timeout) can make shutdown return in time.
func slowWriteConfig() *config.Connector {
	cfg := onlineWriterConfig()
	cfg.Kafka.WriteTimeout = time.Minute
	cfg.Kafka.ReadTimeout = time.Minute
	return cfg
}

// TestOnlineWriterCloseAbortsStalledProduce proves Close bounds a stalled
// Produce: kafka-go uses a background context plus WriteTimeout, so without
// aborting the owned transport the writer wait group would block for a minute.
func TestOnlineWriterCloseAbortsStalledProduce(t *testing.T) {
	stalled := newStalledTransport()
	writer, err := newOnlineWriter(slowWriteConfig(), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1}, stalled)
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writer.WriteMessages(context.Background(), gokafka.Message{Topic: "topic", Value: []byte("value")})
	}()
	select {
	case <-stalled.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the write never reached the stalled transport")
	}

	started := time.Now()
	require.NoError(t, writer.Close())
	require.Less(t, time.Since(started), 3*time.Second, "Close must abort the stalled broker request instead of waiting for WriteTimeout")

	select {
	case writeErr := <-writeDone:
		require.Error(t, writeErr, "an aborted write can never be reported as delivered")
		require.True(t,
			errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) || errors.Is(writeErr, io.ErrClosedPipe),
			"unexpected aborted-write error: %v", writeErr)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight write did not return after Close")
	}
	require.Equal(t, 1, stalled.closeIdleCount(), "the owned transport is released exactly once")
}

// TestOnlineWriterLifetimeAbortsInFlightWrite proves the lifetime seam (used by
// the connector before it asks core to stop) aborts broker I/O.
func TestOnlineWriterLifetimeAbortsInFlightWrite(t *testing.T) {
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	stalled := newStalledTransport()
	writer, err := newOnlineWriter(slowWriteConfig(), OnlineWriterOptions{Lifetime: lifetime, BatchBytes: 1 << 20, MaxAttempts: 1}, stalled)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writer.WriteMessages(context.Background(), gokafka.Message{Topic: "topic", Value: []byte("value")})
	}()
	select {
	case <-stalled.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the write never reached the stalled transport")
	}

	stop()
	select {
	case writeErr := <-writeDone:
		require.Error(t, writeErr)
		require.True(t,
			errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded),
			"the lifetime must surface as an aborted write, got %v", writeErr)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight write ignored the writer lifetime")
	}

	started := time.Now()
	require.NoError(t, writer.Close())
	require.Less(t, time.Since(started), time.Second)
}

// TestOnlineWriterCloseIsIdempotentAndConcurrent proves concurrent Close calls
// join one cleanup and release the owned transport once.
func TestOnlineWriterCloseIsIdempotentAndConcurrent(t *testing.T) {
	stalled := newStalledTransport()
	writer, err := newOnlineWriter(slowWriteConfig(), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1}, stalled)
	require.NoError(t, err)

	closeErrs := make(chan error, 4)
	var waitGroup sync.WaitGroup
	for i := 0; i < 4; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			closeErrs <- writer.Close()
		}()
	}
	finished := make(chan struct{})
	go func() { waitGroup.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Close calls did not finish")
	}
	close(closeErrs)
	for closeErr := range closeErrs {
		require.NoError(t, closeErr)
	}
	require.Equal(t, 1, stalled.closeIdleCount())
	require.ErrorIs(t, writer.WriteMessages(context.Background(), gokafka.Message{Topic: "topic"}), io.ErrClosedPipe)
}

// TestOnlineRoundTripperRefusesAfterAbort proves no new broker request starts
// once the writer lifetime ended, and that an in-flight call is cancelled.
func TestOnlineRoundTripperRefusesAfterAbort(t *testing.T) {
	lifetime, stop := context.WithCancel(context.Background())
	stalled := newStalledTransport()
	wrapped := &onlineRoundTripper{inner: stalled, abort: lifetime}

	done := make(chan error, 1)
	go func() {
		_, err := wrapped.RoundTrip(context.Background(), nil, nil)
		done <- err
	}()
	select {
	case <-stalled.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("round trip never started")
	}
	stop()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight round trip was not aborted")
	}

	_, err := wrapped.RoundTrip(context.Background(), nil, nil)
	require.ErrorIs(t, err, context.Canceled, "an aborted writer must refuse new round trips")
}
