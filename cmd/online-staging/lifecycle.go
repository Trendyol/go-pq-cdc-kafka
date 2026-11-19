package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
)

var (
	errStartup  = errors.New("startup")
	errCapture  = errors.New("capture")
	errHealth   = errors.New("health")
	errShutdown = errors.New("shutdown")
)

type captureLifecycle interface {
	Start(context.Context)
	WaitUntilReady(context.Context) error
	Close()
	Done() <-chan struct{}
	Err() error
	Health() cdc.OnlineHealth
}

type captureFactory func(context.Context, runtimeConfig, *atomic.Bool, io.Writer) (captureLifecycle, error)

// Backend arguments/messages can include connection strings, credentials and
// row images. Only a constant component and level are emitted; not a blacklist.
type safeCaptureLogger struct {
	output io.Writer
	mu     sync.Mutex
}

func (l *safeCaptureLogger) Debug(string, ...any) {}
func (l *safeCaptureLogger) Info(string, ...any)  {}
func (l *safeCaptureLogger) Warn(string, ...any)  { l.write("warn") }
func (l *safeCaptureLogger) Error(string, ...any) { l.write("error") }
func (l *safeCaptureLogger) write(level string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Logging is best effort and contains only bounded compile-time fields.
	_ = json.NewEncoder(l.output).Encode(map[string]string{"component": "capture", "level": level})
}

func newCapture(ctx context.Context, cfg runtimeConfig, failed *atomic.Bool, output io.Writer) (captureLifecycle, error) {
	connector, err := cdc.NewConnector(ctx, cfg.Connector, canaryHandler(cfg.KeyColumn, failed), cdc.WithLogger(&safeCaptureLogger{output: output}))
	if err != nil {
		return nil, errStartup
	}
	online, ok := connector.(cdc.OnlineConnector)
	if !ok {
		connector.Close()
		return nil, errStartup
	}
	return online, nil
}

const runnerStateFailed = "failed"

type runnerStatus struct {
	ready    atomic.Bool
	stopping atomic.Bool
	failed   atomic.Bool
}

func (s *runnerStatus) handler(ctx context.Context, capture captureLifecycle) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/livez" && request.URL.Path != "/readyz" {
			http.NotFound(writer, request)
			return
		}
		running := capture.Health().Running
		done := false
		select {
		case <-capture.Done():
			done = true
		default:
		}
		ready := s.ready.Load() && !s.stopping.Load() && !s.failed.Load() && running && !done && capture.Err() == nil && ctx.Err() == nil
		state := "starting"
		if ready {
			state = "running"
		}
		if s.stopping.Load() || ctx.Err() != nil {
			state = "stopping"
		}
		if s.failed.Load() || capture.Err() != nil {
			state = runnerStateFailed
		}
		code := http.StatusOK
		if request.URL.Path == "/readyz" && !ready || state == runnerStateFailed || done || s.stopping.Load() {
			code = http.StatusServiceUnavailable
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(code)
		category := ""
		if state == runnerStateFailed {
			category = "capture"
		}
		_ = json.NewEncoder(writer).Encode(struct {
			State    string `json:"state"`
			Ready    bool   `json:"ready"`
			Category string `json:"category"`
		}{state, ready, category})
	})
}

func runCapture(ctx context.Context, cfg runtimeConfig, factory captureFactory, output io.Writer) (result error) {
	var transformFailed atomic.Bool
	capture, err := factory(ctx, cfg, &transformFailed, output)
	if err != nil {
		return errStartup
	}
	status := &runnerStatus{}
	defer func() {
		status.stopping.Store(true)
		status.ready.Store(false)
		capture.Close()
		if result == nil && (capture.Health().State == cdc.OnlineStateFailed || errors.Is(capture.Err(), cdc.ErrOnlineShutdownIncomplete)) {
			result = errShutdown
		}
	}()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HealthAddress)
	if err != nil {
		return errHealth
	}
	server := &http.Server{Handler: status.handler(ctx, capture), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Serve(listener) }()
	defer func() {
		status.stopping.Store(true)
		status.ready.Store(false)
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(cleanup); err != nil {
			_ = server.Close() // bounded fallback releases the owned listener/connections
			if result == nil {
				result = errShutdown
			}
		}
	}()
	capture.Start(ctx)
	readyCtx, cancelReady := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancelReady()
	readyResult := make(chan error, 1)
	go func() { readyResult <- capture.WaitUntilReady(readyCtx) }()
	return awaitCapture(ctx, capture, status, readyResult, serverResult, &transformFailed)
}

func awaitCapture(ctx context.Context, capture captureLifecycle, status *runnerStatus, readyResult <-chan error, serverResult <-chan error, transformFailed *atomic.Bool) error {
	for {
		select {
		case <-ctx.Done():
			return captureCompletionResult(ctx, capture, status, readyResult, serverResult, transformFailed)
		case <-capture.Done():
			return captureCompletionResult(ctx, capture, status, readyResult, serverResult, transformFailed)
		case err := <-serverResult:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return errHealth
			}
			return errHealth // owned health service cannot disappear while capture runs
		case err := <-readyResult:
			if err != nil {
				if transformFailed.Load() {
					return errTransform
				}
				if ctx.Err() != nil && cancellationOnly(err) {
					return captureCompletionResult(ctx, capture, status, nil, serverResult, transformFailed)
				}
				return errStartup
			}
			status.ready.Store(true)
			readyResult = nil
		}
	}
}

// Either cancellation or capture completion can win the select during a signal
// stop. Only an operator-cancelled, otherwise successful capture is cooperative;
// joined failures and transformation errors must remain failures.
func captureCompletionResult(ctx context.Context, capture captureLifecycle, status *runnerStatus, readyResult <-chan error, serverResult <-chan error, transformFailed *atomic.Bool) error {
	if transformFailed.Load() {
		status.failed.Store(true)
		return errTransform
	}
	if ctx.Err() != nil && cancellationOnly(capture.Err()) {
		// Cancellation must not hide an already-completed service or startup
		// failure just because a different ready select case won.
		select {
		case <-serverResult:
			status.failed.Store(true)
			return errHealth
		default:
		}
		select {
		case err := <-readyResult:
			if err != nil && !cancellationOnly(err) {
				status.failed.Store(true)
				return errStartup
			}
		default:
		}
		return nil
	}
	status.failed.Store(true)
	return errCapture
}

func cancellationOnly(err error) bool {
	if err == nil {
		return true
	}
	var multiple interface{ Unwrap() []error }
	if errors.As(err, &multiple) {
		causes := multiple.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationOnly(cause) {
				return false
			}
		}
		return true
	}
	var single interface{ Unwrap() error }
	if errors.As(err, &single) {
		cause := single.Unwrap()
		return cause != nil && cancellationOnly(cause)
	}
	return errors.Is(err, context.Canceled)
}
