package integration

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	Infra *TestInfrastructure
)

var (
	PostgresTestImage = "POSTGRES_TEST_IMAGE"
	defaultVersion    = "16.2"
)

// KafkaTestImage pins the official Apache Kafka image from Docker Hub. The
// previous Redpanda image could not be pulled through the local registry TLS
// chain; this tag is published multi-arch (linux/arm64 used here).
const KafkaTestImage = "apache/kafka:3.9.1"

// kafkaHostPort is fixed because the broker has to advertise the very endpoint a
// host-side client dials. Scope allows serial integration runs (go test -p=1);
// a taken port fails startup instead of silently rewriting the advertised
// listener.
const kafkaHostPort = "9092"

// fixtureLabels mark this session's containers so verified cleanup can be
// checked against the protected foreign inventory instead of guessing by image.
var fixtureLabels = map[string]string{"cdc.test.owner": "go-pq-cdc-kafka-online"}

// fixtureTopics are the destinations the integration tests address by name. The
// legacy default response handler treats UnknownTopicOrPartition as fatal, so
// topics are provisioned up front instead of relying on broker-side creation
// during the first produce request.
var fixtureTopics = []string{
	"batch.handler.test",
	"books.test.snapshot",
	"failed.flush.test",
	"lsn.events.test",
	"oversized.test.mixed",
	"oversized.test.partition",
	"oversized.test.stall",
	"products.test.ack",
	"tx.metadata.test",
	"users.test",
	"users.test.delete",
	"users.test.update",
}

func TestMain(m *testing.M) {
	// os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") // Podman: disable Ryuk

	ctx := context.Background()
	var err error

	// Setup test infrastructure
	Infra, err = setupTestInfrastructure(ctx)
	if err != nil {
		log.Fatal("setup test infrastructure:", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if err := Infra.Cleanup(ctx); err != nil {
		log.Printf("cleanup error: %v", err)
	}

	os.Exit(code)
}

func setupTestInfrastructure(ctx context.Context) (*TestInfrastructure, error) {
	// Get PostgreSQL version from environment or use default
	postgresVersion := os.Getenv(PostgresTestImage)
	if postgresVersion == "" {
		postgresVersion = defaultVersion
	}

	// Setup PostgreSQL container
	postgresPort := "5432/tcp"
	postgresReq := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("postgres:%s", postgresVersion),
		ExposedPorts: []string{postgresPort},
		Env: map[string]string{
			"POSTGRES_USER":             "cdc_user",
			"POSTGRES_PASSWORD":         "cdc_pass",
			"POSTGRES_DB":               "cdc_db",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			// Every test creates its own slot and leaves it behind.
			"-c", "max_replication_slots=20",
		},
		Labels: fixtureLabels,
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: postgresReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	infrastructure := &TestInfrastructure{PostgresContainer: postgresContainer}
	fail := func(err error) (*TestInfrastructure, error) {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer stop()
		if cleanupErr := infrastructure.Cleanup(cleanup); cleanupErr != nil {
			return nil, errors.Join(err, fmt.Errorf("own fixture cleanup: %w", cleanupErr))
		}
		return nil, err
	}

	postgresHost, err := postgresContainer.Host(ctx)
	if err != nil {
		return fail(fmt.Errorf("failed to get postgres host: %w", err))
	}

	postgresMappedPort, err := postgresContainer.MappedPort(ctx, nat.Port(postgresPort))
	if err != nil {
		return fail(fmt.Errorf("failed to get postgres mapped port: %w", err))
	}

	// Fail fast while the port is still free: with Ryuk disabled a daemon-level
	// bind failure leaves a created-but-unstarted container behind.
	if err := checkHostPortFree(kafkaHostPort); err != nil {
		return fail(err)
	}

	// Setup Apache Kafka in single-node KRaft mode. The env set is complete
	// because overriding any env var drops the image's config defaults.
	kafkaReq := testcontainers.ContainerRequest{
		Image:        KafkaTestImage,
		ExposedPorts: []string{kafkaHostPort + ":" + kafkaHostPort + "/tcp"},
		Env: map[string]string{
			"CLUSTER_ID":                                     "5L6g3nShT-eMCtK--X86sw",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:" + kafkaHostPort + ",CONTROLLER://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:" + kafkaHostPort,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		},
		Labels: fixtureLabels,
		WaitingFor: wait.ForLog("Kafka Server started").
			WithStartupTimeout(120 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: kafkaReq,
		Started:          true,
	})
	if err != nil {
		return fail(fmt.Errorf("failed to start kafka container (%s on host port %s): %w", KafkaTestImage, kafkaHostPort, err))
	}
	infrastructure.KafkaContainer = kafkaContainer

	// A log line is not a protocol contract: prove the advertised listener
	// answers a real metadata request before any test runs.
	kafkaAddr := net.JoinHostPort("localhost", kafkaHostPort)
	if err := waitForKafkaMetadata(ctx, kafkaAddr, 60*time.Second); err != nil {
		return fail(err)
	}
	if err := ensureFixtureTopics(ctx, kafkaAddr); err != nil {
		return fail(err)
	}

	return &TestInfrastructure{
		PostgresHost:      postgresHost,
		PostgresPort:      postgresMappedPort.Port(),
		KafkaHost:         "localhost",
		KafkaPort:         kafkaHostPort,
		PostgresContainer: postgresContainer,
		KafkaContainer:    kafkaContainer,
	}, nil
}

func checkHostPortFree(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("host port %s is not free; stop the container that publishes it: %w", port, err)
	}
	if err := listener.Close(); err != nil {
		return fmt.Errorf("release host port %s probe: %w", port, err)
	}
	return nil
}

func ensureFixtureTopics(ctx context.Context, addr string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := (&kafka.Dialer{Timeout: 10 * time.Second}).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("kafka topic provisioning dial: %w", err)
	}
	partitions, err := conn.ReadPartitions()
	_ = conn.Close()
	if err != nil {
		return fmt.Errorf("kafka topic provisioning metadata: %w", err)
	}
	existing := make(map[string]bool, len(partitions))
	for _, partition := range partitions {
		existing[partition.Topic] = true
	}
	client := &kafka.Client{Addr: kafka.TCP(addr), Timeout: 30 * time.Second}
	for _, topic := range fixtureTopics {
		if existing[topic] {
			continue
		}
		response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
		})
		if err != nil {
			return fmt.Errorf("create kafka topic %q: %w", topic, err)
		}
		if topicErr := response.Errors[topic]; topicErr != nil {
			return fmt.Errorf("create kafka topic %q: %w", topic, topicErr)
		}
	}
	return nil
}

func waitForKafkaMetadata(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := (&kafka.Dialer{Timeout: 5 * time.Second}).DialContext(dialCtx, "tcp", addr)
		if err == nil {
			_, err = conn.ReadPartitions()
			_ = conn.Close()
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("kafka %s did not answer a metadata request within %s: %w", addr, timeout, lastErr)
}
