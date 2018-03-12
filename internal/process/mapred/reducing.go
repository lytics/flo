package mapred

import (
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/window"
)

func (p *Process) reduce(e graph.Event) error {
	var changes window.State
	mut := func(state window.State) error {
		changes = state
		return p.def.Merge(e.Window, e.Data, state)
	}

	err := p.db.Apply(p.ctx, e.Key, mut)
	if err != nil {
		return err
	}

	return p.def.Track(e.Key, e.Window, e.Data, state)
}
