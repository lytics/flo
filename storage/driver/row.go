package driver

import (
	"github.com/lytics/flo/window"
)

func NewRow(rw ReadWriter) (*Row, error) {
	current, err := rw.Snapshot()
	if err != nil {
		return nil, err
	}
	return &Row{
		rw:      rw,
		updates: map[window.Span]Update{},
		current: current,
	}, nil
}

// Row data for a key.
type Row struct {
	rw      ReadWriter
	updates map[window.Span]Update
	current map[window.Span]Record
}

// Get the values for span s.
func (row *Row) Get(s window.Span) []interface{} {
	up, ok := row.updates[s]
	if ok {
		return up.Record.Values
	}
	rec, ok := row.current[s]
	if ok {
		return rec.Values
	}
	return nil
}

// Coalesce span s by setting its values to vs, and merging
// clock and count data from the set of spans rs, which it
// replaces.
func (row *Row) Coalesce(s window.Span, rs []window.Span, vs []interface{}) {
	row.updates[s] = Update{
		Record: Record{
			Values: vs,
		},
		Span:     s,
		Replaces: rs,
	}
}

// Spans of the row.
func (row *Row) Spans() []window.Span {
	snap := make([]window.Span, 0, len(row.current))
	for s := range row.current {
		snap = append(snap, s)
	}
	return snap
}

// Flush pending changes staged by calls to Coalesce,
// and return a manifest of changes.
func (row *Row) Flush() (map[window.Span]Update, error) {
	manifest := map[window.Span]Update{}
	for s, up := range row.updates {
		rec := row.current[s]
		for _, r := range up.Replaces {
			prev, ok := row.current[r]
			if ok {
				rec.Count += prev.Count
				if rec.Clock < prev.Clock {
					rec.Clock = prev.Clock
				}
			}
		}
		rec.Clock++
		rec.Count++
		rec.Values = up.Record.Values

		manifest[s] = Update{
			Record:   rec,
			Span:     s,
			Replaces: up.Replaces,
		}

		err := row.rw.Set(s, rec)
		if err != nil {
			return nil, err
		}
	}
	for _, up := range row.updates {
		for _, r := range up.Replaces {
			err := row.rw.Del(r)
			if err != nil {
				return nil, err
			}
		}
	}
	return manifest, nil
}
