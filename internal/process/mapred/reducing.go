package mapred

import (
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/window"
)

func (p *Process) reduce(m graph.Event) error {
	mut := func(s window.State) error {
		err := p.def.Merge(&m, s)
		if err != nil {
			return err
		}
		return p.def.Trigger().Modified(m.Key, m.Data, s.Spans())
	}
	return p.db.Apply(p.ctx, m.Key, mut)
}
