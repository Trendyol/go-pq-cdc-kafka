package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
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
			"-c", "max_replication_slots=10",
		},
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

	postgresHost, err := postgresContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres host: %w", err)
	}

	postgresMappedPort, err := postgresContainer.MappedPort(ctx, nat.Port(postgresPort))
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres mapped port: %w", err)
	}

	// Setup Redpanda (Kafka-compatible) container
	kafkaReq := testcontainers.ContainerRequest{
		Image:        "docker.redpanda.com/redpandadata/redpanda:v23.1.12",
		ExposedPorts: []string{"9092:9092/tcp"},
		Cmd:          []string{"redpanda", "start"},
		WaitingFor: wait.ForLog("Successfully started Redpanda").
			WithStartupTimeout(60 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: kafkaReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka container: %w", err)
	}

	// Get the actual mapped port
	kafkaMappedPort, err := kafkaContainer.MappedPort(ctx, "9092")
	if err != nil {
		return nil, fmt.Errorf("failed to get kafka mapped port: %w", err)
	}

	return &TestInfrastructure{
		PostgresHost:      postgresHost,
		PostgresPort:      postgresMappedPort.Port(),
		KafkaHost:         "localhost",
		KafkaPort:         kafkaMappedPort.Port(),
		PostgresContainer: postgresContainer,
		KafkaContainer:    kafkaContainer,
	}, nil
}
