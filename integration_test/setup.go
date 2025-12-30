package integration

import (
	"context"
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

func (ti *TestInfrastructure) Cleanup(ctx context.Context) error {
	if ti.KafkaContainer != nil {
		if err := ti.KafkaContainer.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate kafka container: %w", err)
		}
	}
	if ti.PostgresContainer != nil {
		if err := ti.PostgresContainer.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate postgres container: %w", err)
		}
	}
	return nil
}
