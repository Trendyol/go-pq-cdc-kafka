package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitUntilReadyReturnsClosedWhenReadyPendingInBuffer(t *testing.T) {
	c := &connector{
		readyCh:  make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}

	c.readyCh <- struct{}{}
	close(c.closedCh)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector closed")
}

func TestWaitUntilReadyReturnsReadyWhenNotClosed(t *testing.T) {
	c := &connector{
		readyCh:  make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}

	c.readyCh <- struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, c.WaitUntilReady(ctx))
}

func TestWaitUntilReadyReturnsClosedAfterClose(t *testing.T) {
	c := &connector{
		readyCh:  make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}

	close(c.closedCh)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector closed")
}

func TestIsClosedReturnsFalseForOpenConnector(t *testing.T) {
	c := &connector{
		closedCh: make(chan struct{}),
	}

	assert.False(t, c.isClosed())
}

func TestIsClosedReturnsTrueAfterClose(t *testing.T) {
	c := &connector{
		closedCh: make(chan struct{}),
	}

	close(c.closedCh)
	assert.True(t, c.isClosed())
}
