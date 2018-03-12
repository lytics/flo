package window

import (
	"time"

	"github.com/lytics/flo/merger"
)

// State of a key's windows and associated values.
type State interface {
	Get(Span) []interface{}
	Spans() []Span
	Coalesce(Span, []Span, []interface{})
}

// Window strategy.
type Window interface {
	Apply(ts time.Time) []Span
	Merge(s Span, v interface{}, prev State, f merger.ManyMerger) error
}
