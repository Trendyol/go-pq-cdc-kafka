package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	pkgerrors "github.com/pkg/errors"
	gokafka "github.com/segmentio/kafka-go"
)

// OnlineWriterOptions are the already-admitted bounds of the online path. The
// caller validates them (config policy lives in the connector), the writer only
// applies them to one synchronous single-message kafka.Writer.
type OnlineWriterOptions struct {
	// Lifetime aborts in-flight broker I/O and refuses new round trips once it
	// is done. kafka-go starts Produce with its own background context and keeps
	// retrying, so this is the only seam that can stop a stalled request during
	// shutdown. Nil means the writer only aborts on Close.
	Lifetime context.Context
	// BatchBytes is the hard protocol size limit for one message. A larger
	// message is refused by kafka.Writer before any request is sent.
	BatchBytes int64
	// MaxAttempts bounds internal retries of one WriteMessages call.
	MaxAttempts int
}

// onlineTransport is the broker boundary this writer owns. Production uses
// kafka.Transport; tests substitute a controlled stall. It stays private: it can
// never turn an unacknowledged write into a successful one.
type onlineTransport interface {
	gokafka.RoundTripper
	CloseIdleConnections()
}

// onlineRoundTripper couples the owned transport with the writer lifetime.
// kafka-go's Produce uses context.Background() plus its own WriteTimeout, so
// caller cancellation alone cannot stop it; this wrapper cancels the round trip
// as soon as the lifetime ends, which also lets Writer.Close's wait group finish.
type onlineRoundTripper struct {
	inner onlineTransport
	abort context.Context
}

func (t *onlineRoundTripper) RoundTrip(ctx context.Context, addr net.Addr, request gokafka.Request) (gokafka.Response, error) {
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(t.abort, cancel)
	defer stop()
	if err := t.abort.Err(); err != nil {
		return nil, fmt.Errorf("online kafka transport aborted: %w", err)
	}
	return t.inner.RoundTrip(call, addr, request)
}

func (t *onlineRoundTripper) CloseIdleConnections() { t.inner.CloseIdleConnections() }

// dialFunc is the socket dialing boundary this writer owns.
type dialFunc func(context.Context, string, string) (net.Conn, error)

// errOnlineSocketsActive reports owned sockets that were still open when the
// drain budget expired. It is unexported so no caller can treat an incomplete
// socket teardown as a certified clean close.
var errOnlineSocketsActive = errors.New("online kafka owned sockets still active")

// onlineSocketDrainTimeout bounds the drain a caller-visible Close performs when
// no explicit budget is supplied.
const onlineSocketDrainTimeout = 2 * time.Second

// onlineSocket is one dialed connection. Closing it deregisters from the owned
// set, whether this writer, the transport or the peer closed it first.
type onlineSocket struct {
	net.Conn
	set   *onlineSocketSet
	once  sync.Once
	owner string
}

func (s *onlineSocket) Close() error {
	err := s.Conn.Close()
	s.once.Do(func() { s.set.remove(s) })
	return err
}

// onlineSocketSet owns every socket this writer dialed. Abort actively closes
// them: cancelling a request context only detaches the caller, while kafka-go's
// connection worker keeps using the socket until its own WriteTimeout deadline.
type onlineSocketSet struct {
	conns   map[*onlineSocket]struct{}
	changed chan struct{}
	abort   context.Context
	mu      sync.Mutex
	aborted bool
}

func newOnlineSocketSet(abort context.Context) *onlineSocketSet {
	return &onlineSocketSet{conns: make(map[*onlineSocket]struct{}), changed: make(chan struct{}), abort: abort}
}

// dial dials through the owned boundary. A dial that finishes after abort is
// refused instead of being registered, so no connection can outlive shutdown.
func (s *onlineSocketSet) dial(inner dialFunc) dialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		call, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(s.abort, cancel)
		defer stop()
		if err := s.abort.Err(); err != nil {
			return nil, fmt.Errorf("online kafka dial refused after shutdown: %w", err)
		}
		conn, err := inner(call, network, address)
		if err != nil {
			return nil, err
		}
		socket := &onlineSocket{Conn: conn, set: s, owner: address}
		if !s.register(socket) {
			_ = conn.Close()
			return nil, fmt.Errorf("online kafka dial completed after shutdown: %w", context.Canceled)
		}
		return socket, nil
	}
}

func (s *onlineSocketSet) register(socket *onlineSocket) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aborted {
		return false
	}
	s.conns[socket] = struct{}{}
	s.notifyLocked()
	return true
}

func (s *onlineSocketSet) remove(socket *onlineSocket) {
	s.mu.Lock()
	delete(s.conns, socket)
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *onlineSocketSet) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// abortSockets refuses new dials and actively closes every owned socket.
func (s *onlineSocketSet) abortSockets() {
	s.mu.Lock()
	if s.aborted {
		s.mu.Unlock()
		return
	}
	s.aborted = true
	sockets := make([]*onlineSocket, 0, len(s.conns))
	for socket := range s.conns {
		sockets = append(sockets, socket)
	}
	s.notifyLocked()
	s.mu.Unlock()

	for _, socket := range sockets {
		_ = socket.Conn.Close() // wake the blocked request immediately
		_ = socket.Close()      // deregister
	}
}

func (s *onlineSocketSet) active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// waitDrained waits until every owned socket is released or the budget expires.
func (s *onlineSocketSet) waitDrained(ctx context.Context) error {
	for {
		s.mu.Lock()
		remaining := len(s.conns)
		changed := s.changed
		s.mu.Unlock()
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			s.mu.Lock()
			stillActive := len(s.conns)
			s.mu.Unlock()
			return fmt.Errorf("%w: %d connection(s): %w", errOnlineSocketsActive, stillActive, ctx.Err())
		case <-changed:
		}
	}
}

// OnlineWriter writes exactly one message per call and returns only after the
// broker acknowledged it with acks=all. It owns its transport, and Close aborts
// in-flight broker I/O before releasing the writer, so a stalled request cannot
// hold shutdown open past the caller's budget.
type OnlineWriter struct {
	writer    *gokafka.Writer
	transport onlineTransport
	sockets   *onlineSocketSet
	abort     context.CancelFunc
	closeErr  error
	socketErr error
	closeOnce sync.Once
}

// NewOnlineWriter builds a synchronous, acks-all writer. It performs no network
// I/O, so construction cannot hide an unreachable broker as a startup failure.
func NewOnlineWriter(cfg *config.Connector, options OnlineWriterOptions) (*OnlineWriter, error) {
	if cfg.Kafka.RequiredAcks != -1 {
		return nil, errors.New("online kafka writer requires requiredAcks: -1 (acks=all)")
	}
	if options.MaxAttempts < 1 {
		return nil, errors.New("online kafka writer requires MaxAttempts >= 1")
	}
	if options.BatchBytes < 1 {
		return nil, errors.New("online kafka writer requires a positive BatchBytes")
	}

	return newOnlineWriterWithDialer(cfg, options, (&net.Dialer{}).DialContext)
}

func newOnlineTransport(cfg *config.Connector) (*gokafka.Transport, error) {
	transport := &gokafka.Transport{
		MetadataTTL:    cfg.Kafka.MetadataTTL,
		MetadataTopics: cfg.Kafka.MetadataTopics,
		ClientID:       cfg.Kafka.ClientID,
	}
	if cfg.Kafka.SecureConnection {
		tlsContent, err := newTLSContent(cfg.Kafka.ScramUsername, cfg.Kafka.ScramPassword, cfg.Kafka.RootCA, cfg.Kafka.InterCA)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "online kafka TLS/SASL")
		}
		transport.TLS = tlsContent.config
		transport.SASL = tlsContent.sasl
	}
	return transport, nil
}

// newOnlineWriterWithDialer wires the production transport with the owned dialing
// boundary. Tests supply a controlled dialer to exercise the abort race.
func newOnlineWriterWithDialer(cfg *config.Connector, options OnlineWriterOptions, dial dialFunc) (*OnlineWriter, error) {
	transport, err := newOnlineTransport(cfg)
	if err != nil {
		return nil, err
	}
	return buildOnlineWriter(cfg, options, transport, dial)
}

// newOnlineWriter keeps the low-level double seam used by the abort/wait tests.
// A substituted transport owns its own sockets, so this path tracks none.
func newOnlineWriter(cfg *config.Connector, options OnlineWriterOptions, transport onlineTransport) (*OnlineWriter, error) {
	return buildOnlineWriter(cfg, options, transport, nil)
}

func buildOnlineWriter(cfg *config.Connector, options OnlineWriterOptions, transport onlineTransport, dial dialFunc) (*OnlineWriter, error) {
	lifetime := options.Lifetime
	if lifetime == nil {
		lifetime = context.Background()
	}
	abortContext, abort := context.WithCancel(lifetime) //nolint:gosec // abort is stored and called by Close

	// The dialing boundary is only meaningful for the real transport.
	var sockets *onlineSocketSet
	if kafkaTransport, ok := transport.(*gokafka.Transport); ok && dial != nil {
		sockets = newOnlineSocketSet(abortContext)
		kafkaTransport.Dial = sockets.dial(dial)
	}

	return &OnlineWriter{
		transport: transport,
		sockets:   sockets,
		abort:     abort,
		writer: &gokafka.Writer{
			Addr:         gokafka.TCP(cfg.Kafka.Brokers...),
			Balancer:     cfg.Kafka.GetBalancer(),
			RequiredAcks: gokafka.RequireAll,
			Compression:  gokafka.Compression(cfg.Kafka.GetCompression()),
			BatchSize:    1,
			// One message per request: BatchSize 1 flushes immediately, so the
			// batch timeout can never delay or coalesce the acknowledgement.
			BatchTimeout:           time.Millisecond,
			BatchBytes:             options.BatchBytes,
			MaxAttempts:            options.MaxAttempts,
			ReadTimeout:            cfg.Kafka.ReadTimeout,
			WriteTimeout:           cfg.Kafka.WriteTimeout,
			AllowAutoTopicCreation: false,
			Transport:              &onlineRoundTripper{inner: transport, abort: abortContext},
		},
	}, nil
}

// WriteMessages blocks until the broker acknowledged the message. The returned
// error is the broker/transport error; after ctx cancellation kafka-go makes no
// promise about whether the message was appended, so callers must treat a
// context error as an unknown outcome and not as a confirmed failure.
func (w *OnlineWriter) WriteMessages(ctx context.Context, messages ...gokafka.Message) error {
	return w.writer.WriteMessages(ctx, messages...)
}

// Close aborts in-flight broker I/O, actively closes every owned socket and then
// releases the writer. Cancelling the request context alone only detaches the
// caller: kafka-go's connection worker keeps using the socket until its own
// WriteTimeout, so the owned sockets are closed here. Close is idempotent and
// reports an incomplete socket teardown instead of claiming a clean close.
func (w *OnlineWriter) Close() error { return w.closeWithin(context.Background()) }

// closeWithin performs the shutdown inside the caller's budget.
func (w *OnlineWriter) closeWithin(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.abort()
		if w.sockets != nil {
			w.sockets.abortSockets()
		}
		// The writer's wait group can now finish: its in-flight Produce lost the
		// socket and queued batches fail fast against the refused dialer.
		w.closeErr = w.writer.Close()
		if w.sockets != nil {
			drain, cancel := context.WithTimeout(ctx, onlineSocketDrainTimeout)
			defer cancel()
			w.socketErr = w.sockets.waitDrained(drain)
		}
		w.transport.CloseIdleConnections()
	})
	return errors.Join(w.closeErr, w.socketErr)
}
