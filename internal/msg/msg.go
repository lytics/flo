package msg

import (
	"time"

	"github.com/lytics/grid"
)

// EventTime of message.
func (m *Keyed) EventTime() time.Time {
	return time.Unix(m.TS, 0)
}

func init() {
	grid.Register(Term{})
	grid.Register(Keyed{})
	grid.Register(Progress{})
}
