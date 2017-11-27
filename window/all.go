package window

import (
	"time"

	"github.com/lytics/flo/merger"
)

// All of time window, which is considered to be the window of
// time between the dates:
//
//             0001-01-01 00:00:00 +0000 UTC
//     292277026596-01-01 00:00:00 +0000 UTC
//
// Where the max is about 292 billion years in the future.
func All() Window {
	min := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	max := time.Date(292277026596, 1, 1, 0, 0, 0, 0, time.UTC)
	return &all{
		universe: NewSpan(min, max),
	}
}

type all struct {
	universe Span
}

func (w *all) Apply(time.Time) []Span {
	return []Span{w.universe}
}

// Merge the new value vs into the universal window
// in ss.
func (w *all) Merge(s Span, v interface{}, ss State, f merger.ManyMerger) error {
	vs := []interface{}{v}
	vs0 := ss.Get(w.universe)
	vs2, err := f(vs, vs0)
	if err != nil {
		return err
	}
	ss.Set(w.universe, vs2)
	return nil
}
