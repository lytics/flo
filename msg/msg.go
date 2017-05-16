package msg

import (
	"time"

	"github.com/lytics/grid/grid.v3"
)

func (m *Keyed) TS() time.Time {
	return time.Unix(m.Unix, 0)
}

func init() {
	grid.Register(Unit{})
	grid.Register(Keyed{})
}
