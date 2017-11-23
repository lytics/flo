package msg

import (
	"time"

	"github.com/lytics/grid"
)

// EventTime of message.
func (m *Event) EventTime() time.Time {
	return time.Unix(m.Time, 0)
}

func init() {
	grid.Register(Term{})
	grid.Register(Event{})
	grid.Register(Progress{})
}
