package window

import (
	"time"

	"github.com/lytics/flo/merger"
)

// State of span and its values.
type State interface {
	Del(Span)
	Get(Span) []interface{}
	Set(Span, []interface{})
	Spans() map[Span][]interface{}
}

// Window strategy.
type Window interface {
	Apply(ts time.Time) []Span
	Merge(ts time.Time, vs []interface{}, ss State, f merger.ManyMerger) error
}
