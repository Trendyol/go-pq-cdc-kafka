package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cancellationCompletedCapture models the real connector completing Done from
// cancellation, rather than waiting for the runner's deferred Close call.
type cancellationCompletedCapture struct {
	*controlledCapture
	completed chan struct{}
}

func (c *cancellationCompletedCapture) Start(ctx context.Context) {
	c.controlledCapture.Start(ctx)
	go func() {
		<-ctx.Done()
		c.fail(context.Canceled)
		close(c.completed)
	}()
}

func TestOnlineStagingCancellationAndCaptureDoneAreCooperative(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	capture := &cancellationCompletedCapture{controlledCapture: testCapture(), completed: make(chan struct{})}
	capture.Start(ctx)
	cancel()
	select {
	case <-capture.completed:
	case <-time.After(time.Second):
		t.Fatal("capture did not complete from cancellation")
	}

	// Both select cases are ready; neither may turn a normal signal stop into a
	// capture failure. Repetition exercises either selected branch.
	for range 128 {
		status := &runnerStatus{}
		var transformFailed atomic.Bool
		require.NoError(t, awaitCapture(ctx, capture, status, nil, nil, &transformFailed))
		require.False(t, status.failed.Load())
	}
}

func TestOnlineStagingCancellationDoesNotHideTerminalFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		cause     error
		transform bool
		want      error
	}{
		{"capture", errors.New("capture failed"), false, errCapture},
		{"joined", errors.Join(context.Canceled, errors.New("broker failed")), false, errCapture},
		{"transform", context.Canceled, true, errTransform},
		{"deadline", context.DeadlineExceeded, false, errCapture},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			capture := testCapture()
			capture.fail(test.cause)
			cancel()
			for range 128 {
				status := &runnerStatus{}
				var transformFailed atomic.Bool
				transformFailed.Store(test.transform)
				require.ErrorIs(t, awaitCapture(ctx, capture, status, nil, nil, &transformFailed), test.want)
				require.True(t, status.failed.Load())
			}
		})
	}
}

func TestOnlineStagingWrappedCancellationIsCooperative(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	capture := testCapture()
	capture.fail(fmt.Errorf("capture stopped: %w", context.Canceled))
	cancel()
	for range 128 {
		var transformFailed atomic.Bool
		require.NoError(t, awaitCapture(ctx, capture, &runnerStatus{}, nil, nil, &transformFailed))
	}
}

func TestOnlineStagingCancellationDoesNotHideHealthFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	capture := testCapture()
	capture.fail(context.Canceled)
	cancel()
	for range 128 {
		serverResult := make(chan error, 1)
		serverResult <- errors.New("health server failed")
		readyResult := make(chan error, 1)
		readyResult <- context.Canceled
		var transformFailed atomic.Bool
		err := awaitCapture(ctx, capture, &runnerStatus{}, readyResult, serverResult, &transformFailed)
		require.ErrorIs(t, err, errHealth, "cancellation must not hide an already-failed health service")
	}
}
