package driver

import (
	"github.com/lytics/flo/window"
)

// Driver for datastore operations on time windows
// and their associated data.
type Driver interface {
	Open(name string) (Conn, error)
}

type Conn interface {
	Apply(key string, mut func(window.State) error) error
	Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error)
}

type ReadWriter interface {
	DelSpan(s window.Span) error
	PutSpan(s window.Span, vs []interface{}) error
	Windows() (map[window.Span][]interface{}, error)
}
