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
	Apply(ctx context.Context, key string, mut Mutation) (map[window.Span]Update, error)
}

// Record of data.
type Record struct {
	Clock  int64
	Count  int64
	Values []interface{}
}

// Update manifest.
type Update struct {
	Record   Record
	Span     window.Span
	Replaces []window.Span
}

// ReadWriter of single row data.
type ReadWriter interface {
	Del(s window.Span) error
	Set(s window.Span, rec Record) error
	Snapshot() (map[window.Span]Record, error)
}

// Mutation of window state.
type Mutation func(window.State) error
