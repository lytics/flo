package msg

import (
	"time"

	"github.com/lytics/flo/window"
	"github.com/lytics/grid"
)

// EventTime of message.
func (m *Event) EventTime() window.Span {
	return window.NewSpan(time.Unix(m.SpanStart, 0), time.Unix(m.SpanEnd, 0))
}

func init() {
	grid.Register(Term{})
	grid.Register(Event{})
	grid.Register(Progress{})
}
