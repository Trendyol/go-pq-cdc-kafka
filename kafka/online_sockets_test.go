package kafka

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/produce"

	"github.com/stretchr/testify/require"
)

// fakeProtocolBroker speaks real Kafka protocol framing: it negotiates API
// versions, answers metadata, then accepts a Produce request with acks=all and
// deliberately withholds the response. It is a test-owned fixture; nothing here
// can turn an unacknowledged write into a success.
type fakeProtocolBroker struct {
	listener        net.Listener
	topic           string
	produceSeen     chan struct{}
	eofAfterProduce chan struct{}
	mu              sync.Mutex
	conns           []net.Conn
	acks            []int16
	produceOnce     sync.Once
	eofOnce         sync.Once
}

func newFakeProtocolBroker(t *testing.T, topic string) *fakeProtocolBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	broker := &fakeProtocolBroker{
		listener:        listener,
		topic:           topic,
		produceSeen:     make(chan struct{}),
		eofAfterProduce: make(chan struct{}),
	}
	go broker.serve()
	t.Cleanup(broker.close)
	return broker
}

func (b *fakeProtocolBroker) addr() string { return b.listener.Addr().String() }

func (b *fakeProtocolBroker) serve() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns = append(b.conns, conn)
		b.mu.Unlock()
		go b.handle(conn)
	}
}

func (b *fakeProtocolBroker) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		apiVersion, correlationID, _, message, err := protocol.ReadRequest(conn)
		if err != nil {
			return
		}
		switch request := message.(type) {
		case *apiversions.Request:
			if err := protocol.WriteResponse(conn, apiVersion, correlationID, b.apiVersions()); err != nil {
				return
			}
		case *metadata.Request:
			if err := protocol.WriteResponse(conn, apiVersion, correlationID, b.metadataResponse()); err != nil {
				return
			}
		case *produce.Request:
			b.recordProduce(request)
			// Withhold the response: a correct shutdown must close this socket,
			// which the server observes as EOF instead of waiting for WriteTimeout.
			if _, readErr := conn.Read(make([]byte, 1)); readErr != nil {
				b.eofOnce.Do(func() { close(b.eofAfterProduce) })
			}
			return
		default:
			return
		}
	}
}

func (b *fakeProtocolBroker) apiVersions() protocol.Message {
	keys := []protocol.ApiKey{protocol.ApiVersions, protocol.Metadata, protocol.Produce}
	response := &apiversions.Response{ApiKeys: make([]apiversions.ApiKeyResponse, 0, len(keys))}
	for _, key := range keys {
		response.ApiKeys = append(response.ApiKeys, apiversions.ApiKeyResponse{
			ApiKey:     int16(key),
			MinVersion: key.MinVersion(),
			MaxVersion: key.MaxVersion(),
		})
	}
	return response
}

func (b *fakeProtocolBroker) metadataResponse() protocol.Message {
	host, port, err := net.SplitHostPort(b.listener.Addr().String())
	if err != nil {
		return &metadata.Response{}
	}
	portNumber, _ := net.LookupPort("tcp", port)
	return &metadata.Response{
		Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: host, Port: int32(portNumber)}},
		Topics: []metadata.ResponseTopic{{
			Name: b.topic,
			Partitions: []metadata.ResponsePartition{{
				PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, IsrNodes: []int32{1},
			}},
		}},
	}
}

func (b *fakeProtocolBroker) recordProduce(request *produce.Request) {
	b.mu.Lock()
	for _, topic := range request.Topics {
		b.acks = append(b.acks, request.Acks)
		_ = topic
	}
	b.mu.Unlock()
	b.produceOnce.Do(func() { close(b.produceSeen) })
}

func (b *fakeProtocolBroker) observedAcks() []int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int16(nil), b.acks...)
}

func (b *fakeProtocolBroker) waitProduce(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-b.produceSeen:
	case <-time.After(timeout):
		t.Fatal("no Produce request was negotiated and observed on the wire")
	}
}

// waitEOF waits for the client to close the socket that carried the withheld
// Produce. This only happens if shutdown actively closes owned sockets.
func (b *fakeProtocolBroker) waitEOF(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-b.eofAfterProduce:
	case <-time.After(timeout):
		t.Fatal("the client kept the Produce socket open after shutdown: owned socket was not closed")
	}
}

func (b *fakeProtocolBroker) close() {
	_ = b.listener.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, conn := range b.conns {
		_ = conn.Close()
	}
}

func protocolBrokerConfig(addr string) *config.Connector {
	cfg := onlineWriterConfig()
	cfg.Kafka.Brokers = []string{addr}
	// Much longer than any assertion bound: only an active socket close can end
	// the withheld Produce in time.
	cfg.Kafka.WriteTimeout = 2 * time.Minute
	cfg.Kafka.ReadTimeout = 2 * time.Minute
	return cfg
}

// TestOnlineWriterClosesOwnedProduceSocketOnAbort is the protocol-aware
// regression: after a real version-negotiated Produce with acks=-1 is withheld,
// shutdown must close the owned socket, which the server observes as EOF.
func TestOnlineWriterClosesOwnedProduceSocketOnAbort(t *testing.T) {
	broker := newFakeProtocolBroker(t, "online.socket.topic")
	writer, err := NewOnlineWriter(protocolBrokerConfig(broker.addr()), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1})
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writer.WriteMessages(context.Background(), gokafka.Message{
			Topic: "online.socket.topic", Key: []byte("k"), Value: []byte("v"),
		})
	}()
	broker.waitProduce(t, 15*time.Second)
	require.Equal(t, []int16{-1}, broker.observedAcks(), "the observed request must be a Produce with acks=all")

	started := time.Now()
	require.NoError(t, writer.Close(), "a clean socket teardown must not report incomplete cleanup")
	require.Less(t, time.Since(started), 5*time.Second, "shutdown must not wait for the 2-minute WriteTimeout")

	broker.waitEOF(t, 5*time.Second)

	select {
	case writeErr := <-writeDone:
		require.Error(t, writeErr, "a withheld Produce can never be reported as delivered")
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight write did not return after shutdown")
	}
	require.Zero(t, writer.sockets.active(), "every owned socket must be released before a clean close")
}

// TestOnlineWriterDialAbortRaceLeavesNoSocket aborts while a dial is still in
// progress: the late connection must not be registered or leaked.
func TestOnlineWriterDialAbortRaceLeavesNoSocket(t *testing.T) {
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	blockingDial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(dialStarted)
		select {
		case <-releaseDial:
			return client, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	writer, err := newOnlineWriterWithDialer(protocolBrokerConfig("127.0.0.1:1"), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1}, blockingDial)
	require.NoError(t, err)

	// Exercise the same tracked boundary the transport dials through.
	trackedDial := writer.sockets.dial(blockingDial)
	dialDone := make(chan error, 1)
	go func() {
		_, dialErr := trackedDial(context.Background(), "tcp", "127.0.0.1:1")
		dialDone <- dialErr
	}()
	<-dialStarted
	writer.sockets.abortSockets()
	close(releaseDial)

	select {
	case dialErr := <-dialDone:
		require.Error(t, dialErr, "a dial that completes after abort must be refused")
		require.ErrorIs(t, dialErr, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("dial did not return after abort")
	}
	require.Zero(t, writer.sockets.active(), "an aborted dial must leave no registered socket")
	require.NoError(t, writer.Close())
}

// TestOnlineWriterConcurrentCloseDrainsSockets proves concurrent shutdowns join
// one socket teardown.
func TestOnlineWriterConcurrentCloseDrainsSockets(t *testing.T) {
	broker := newFakeProtocolBroker(t, "online.socket.concurrent")
	writer, err := NewOnlineWriter(protocolBrokerConfig(broker.addr()), OnlineWriterOptions{BatchBytes: 1 << 20, MaxAttempts: 1})
	require.NoError(t, err)

	go func() {
		_ = writer.WriteMessages(context.Background(), gokafka.Message{Topic: "online.socket.concurrent", Value: []byte("v")})
	}()
	broker.waitProduce(t, 15*time.Second)

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
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Close calls did not finish")
	}
	close(closeErrs)
	for closeErr := range closeErrs {
		require.NoError(t, closeErr)
	}
	broker.waitEOF(t, 5*time.Second)
}
