package sink

import (
	"context"

	"github.com/lytics/flo/window"
)

// Sinks of data.
type Sinks interface {
	Setup(graphType, graphName string, conf []byte) ([]Sink, error)
}

// Sink of data.
type Sink interface {
	// Init the sink. Init is called just before
	// a sink is actively going to be used.
	Init() error
	// Stop the sink and clean up. Stop is only
	// called if Init has been called.
	Stop() error
	// Give key and values to sink.
	Give(ctx context.Context, w window.Span, key string, vs []interface{}) error
}
