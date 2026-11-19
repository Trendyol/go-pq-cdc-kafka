package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/stretchr/testify/require"
)

type controlledCapture struct {
	started        chan struct{}
	ready          chan struct{}
	done           chan struct{}
	once           sync.Once
	mu             sync.Mutex
	failure        error
	readyFailure   error
	cleanupFailure error
	running        bool
	closes         int
}

func testCapture() *controlledCapture {
	return &controlledCapture{started: make(chan struct{}), ready: make(chan struct{}), done: make(chan struct{})}
}
func (c *controlledCapture) Start(context.Context) {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	close(c.started)
}
func (c *controlledCapture) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return c.readyFailure
	case <-c.done:
		return c.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *controlledCapture) Close() {
	c.mu.Lock()
	c.closes++
	c.running = false
	if c.cleanupFailure != nil {
		c.failure = c.cleanupFailure
	}
	c.mu.Unlock()
	c.once.Do(func() { close(c.done) })
}
func (c *controlledCapture) Done() <-chan struct{} { return c.done }
func (c *controlledCapture) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.failure }
func (c *controlledCapture) Health() cdc.OnlineHealth {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := cdc.OnlineStateRunning
	if !c.running {
		state = cdc.OnlineStateClosed
	}
	if c.failure != nil {
		state = cdc.OnlineStateFailed
	}
	return cdc.OnlineHealth{State: state, Running: c.running}
}
func (c *controlledCapture) fail(err error) {
	c.mu.Lock()
	c.failure = err
	c.running = false
	c.mu.Unlock()
	c.once.Do(func() { close(c.done) })
}

func TestOnlineStagingReadinessRequiresActualReadyRunningAndNoDone(t *testing.T) {
	capture := testCapture()
	capture.Start(t.Context())
	status := &runnerStatus{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	probe := func(path string) int {
		response := httptest.NewRecorder()
		status.handler(ctx, capture).ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		require.NotContains(t, response.Body.String(), "password")
		return response.Code
	}
	require.Equal(t, http.StatusOK, probe("/livez"))
	require.Equal(t, http.StatusServiceUnavailable, probe("/readyz"))
	status.ready.Store(true)
	require.Equal(t, http.StatusOK, probe("/readyz"))
	capture.fail(errors.New("postgres://password@host/database"))
	require.Equal(t, http.StatusServiceUnavailable, probe("/readyz"), "Done must revoke readiness even after WaitUntilReady succeeded")
	require.Equal(t, http.StatusServiceUnavailable, probe("/livez"))
	cancel()
	require.Equal(t, http.StatusServiceUnavailable, probe("/readyz"))
}

func TestOnlineStagingLifecycleStartupDeliveryCancelAndIncompleteCleanup(t *testing.T) {
	for _, mode := range []string{"startup", "delivery", "cancel", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			capture := testCapture()
			cfg := testRuntimeConfig()
			cfg.HealthAddress = "127.0.0.1:0"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "startup" {
				capture.readyFailure = errors.New("secret startup cause")
			}
			if mode == "incomplete" {
				capture.cleanupFailure = cdc.ErrOnlineShutdownIncomplete
			}
			result := make(chan error, 1)
			go func() {
				result <- runCapture(ctx, cfg, func(context.Context, runtimeConfig, *atomic.Bool, io.Writer) (captureLifecycle, error) {
					return capture, nil
				}, io.Discard)
			}()
			select {
			case <-capture.started:
			case <-time.After(time.Second):
				t.Fatal("capture not started")
			}
			close(capture.ready)
			switch mode {
			case "delivery":
				capture.fail(errors.New("secret delivery cause"))
			case "cancel", "incomplete":
				cancel()
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(3 * time.Second):
				t.Fatal("owned cleanup did not finish")
			}
			switch mode {
			case "startup":
				require.ErrorIs(t, err, errStartup)
			case "delivery":
				require.ErrorIs(t, err, errCapture)
			case "cancel":
				require.NoError(t, err)
			case "incomplete":
				require.ErrorIs(t, err, errShutdown)
			}
			capture.mu.Lock()
			require.Equal(t, 1, capture.closes)
			require.False(t, capture.running)
			capture.mu.Unlock()
		})
	}
}

func TestOnlineStagingHealthBindFailureReleasesUnstartedCapture(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	cfg := testRuntimeConfig()
	cfg.HealthAddress = listener.Addr().String()
	capture := testCapture()
	err = runCapture(t.Context(), cfg, func(context.Context, runtimeConfig, *atomic.Bool, io.Writer) (captureLifecycle, error) {
		return capture, nil
	}, io.Discard)
	require.ErrorIs(t, err, errHealth)
	require.Equal(t, 1, capture.closes)
	select {
	case <-capture.started:
		t.Fatal("failed health bind must not start source capture")
	default:
	}
}

func TestOnlineStagingSafeLoggerNeverLeaksRawOrEncodedSecrets(t *testing.T) {
	var output bytes.Buffer
	log := safeCaptureLogger{output: &output}
	secret := "runtime-password-with-special@/?characters"
	for _, value := range []string{secret, url.QueryEscape(secret), base64.StdEncoding.EncodeToString([]byte(secret))} {
		log.Debug(value, "payload", value)
		log.Info(value, "config", value)
		log.Warn(value, "password", value)
		log.Error(value, "error", errors.New(value))
		require.NotContains(t, output.String(), value)
	}
	require.NotContains(t, output.String(), "payload")
	require.Contains(t, output.String(), `"component":"capture"`)
}

func TestOnlineStagingHealthServiceFailureIsTerminal(t *testing.T) {
	capture := testCapture()
	status := &runnerStatus{}
	serverResult := make(chan error, 1)
	serverResult <- errors.New("health secret")
	var transformFailed atomic.Bool
	err := awaitCapture(t.Context(), capture, status, nil, serverResult, &transformFailed)
	require.ErrorIs(t, err, errHealth)
}
