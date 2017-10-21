package mapred

import (
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/txdb"
)

func (p *Process) reduce(m graph.KeyedEvent) error {
	return p.db.Apply(m.Key, func(row *txdb.Row) error {
		err := p.def.Merge(&m, row)
		if err != nil {
			return err
		}
		return p.def.Trigger().Modified(m.Key, m.Msg, row.Snapshot().Windows)
	})
}
