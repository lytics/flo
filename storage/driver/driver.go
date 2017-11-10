package driver

import (
	"context"

	"github.com/lytics/flo/window"
)

// Driver for datastore operations on time windows
// and their associated data.
type Driver interface {
	Open(name string, cfg Cfg) (Conn, error)
}

// Conn is a handle to a datastore connection.
type Conn interface {
	Apply(ctx context.Context, key string, mut Mutation) error
	Drain(ctx context.Context, keys []string, sink Sink) error
}

// ReadWriter of single row data.
type ReadWriter interface {
	DelSpan(s window.Span) error
	PutSpan(s window.Span, vs []interface{}) error
	Windows() (map[window.Span][]interface{}, error)
}

// Sink for data output.
type Sink func(ctx context.Context, span window.Span, key string, vs []interface{}) error

// Mutation of window state.
type Mutation func(window.State) error
