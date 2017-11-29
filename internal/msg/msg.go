package msg

import (
	"time"

	"github.com/lytics/flo/window"
	"github.com/lytics/grid"
)

// Time of event.
func (m *Event) Time() time.Time {
	return time.Unix(m.TimeUnix, 0)
}

// Window of time the event is associated with.
func (m *Event) Window() window.Span {
	return window.NewSpan(time.Unix(m.WindowStartUnix, 0), time.Unix(m.WindowEndUnix, 0))
}

func init() {
	grid.Register(Term{})
	grid.Register(Event{})
	grid.Register(Progress{})
}
