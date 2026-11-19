package cdc

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
)

// Options are applied to the same private type as the legacy constructor. In
// particular, existing WithLogger/WithPrometheusMetrics cannot type-assert an
// online connector. A response callback is not a durable broker receipt.
func newOnlineFromExistingConstructor(ctx context.Context, cfg config.Connector, handler Handler, options []Option) (Connector, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: online requires a non-nil Handler", ErrOnlineAdmission)
	}
	owned := cloneOnlineConfig(cfg)
	staging := &connector{cfg: &owned, handler: handler}
	for _, option := range options {
		if option != nil {
			option(staging)
		}
	}
	if staging.responseHandlerSet || staging.responseHandler != nil {
		return nil, fmt.Errorf("%w: WithResponseHandler is unsupported online; only owned synchronous acks-all writes certify delivery", ErrOnlineAdmission)
	}
	result, err := newOnlineConnector(ctx, owned, handler)
	if err != nil {
		return nil, err
	}
	result.core.SetMetricCollectors(staging.metrics...)
	return result, nil
}
