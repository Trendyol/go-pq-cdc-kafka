package cdc

import (
	"context"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/logger"
)

// onlineOptions collects the additive online construction settings. It is
// deliberately separate from the legacy connector: the legacy Option func type
// asserts *connector, so reusing it here would either panic or silently accept
// a response handler as a durable receipt.
type onlineOptions struct {
	observer func(context.Context, cdc.OnlineObservation) error
	logger   logger.Logger
}

// OnlineOption customizes NewOnlineConnector. Options cannot weaken the
// admitted write contract: acknowledgement mode, balancer, retry bounds and
// destinations stay validated.
type OnlineOption func(*onlineOptions) error

// WithOnlineLogger installs the core logger for the online capture path.
func WithOnlineLogger(l logger.Logger) OnlineOption {
	return func(options *onlineOptions) error {
		options.logger = l
		return nil
	}
}

// WithOnlineObserver observes core online capture metadata. It is forwarded to
// the core online connector, which calls it synchronously outside its locks and
// stops the run when it returns an error.
func WithOnlineObserver(observer func(context.Context, cdc.OnlineObservation) error) OnlineOption {
	return func(options *onlineOptions) error {
		options.observer = observer
		return nil
	}
}
