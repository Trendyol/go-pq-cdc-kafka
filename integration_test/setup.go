package integration

import (
	"context"
	"errors"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
)

type TestInfrastructure struct {
	PostgresContainer testcontainers.Container
	KafkaContainer    testcontainers.Container
	PostgresHost      string
	PostgresPort      string
	KafkaHost         string
	KafkaPort         string
}

// Cleanup terminates both fixtures even when the first termination fails, so a
// partial start never leaves an own container behind.
func (ti *TestInfrastructure) Cleanup(ctx context.Context) error {
	var err error
	if ti.KafkaContainer != nil {
		if kafkaErr := ti.KafkaContainer.Terminate(ctx); kafkaErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to terminate kafka container: %w", kafkaErr))
		}
	}
	if ti.PostgresContainer != nil {
		if postgresErr := ti.PostgresContainer.Terminate(ctx); postgresErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to terminate postgres container: %w", postgresErr))
		}
	}
	return err
}
