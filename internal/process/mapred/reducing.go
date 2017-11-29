package mapred

import (
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/window"
)

func (p *Process) reduce(e graph.Event) error {
	mut := func(state window.State) error {
		err := p.def.Merge(e.Window, e.Data, state)
		if err != nil {
			return err
		}
		return p.def.Trigger().Modified(e.Key, e.Data, state.Windows())
	}
	return p.db.Apply(p.ctx, e.Key, mut)
}
