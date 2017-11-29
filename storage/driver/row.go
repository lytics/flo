package driver

import (
	"github.com/lytics/flo/window"
)

func NewRow(rw ReadWriter) (*Row, error) {
	windows, err := rw.Windows()
	if err != nil {
		return nil, err
	}
	return &Row{
		rw:      rw,
		windows: windows,
		deletes: map[window.Span]bool{},
		updates: map[window.Span][]interface{}{},
	}, nil
}

// Row data for a key.
type Row struct {
	rw      ReadWriter
	deletes map[window.Span]bool
	updates map[window.Span][]interface{}
	windows map[window.Span][]interface{}
}

func (r *Row) Del(k window.Span) {
	r.deletes[k] = true
}

func (r *Row) Get(k window.Span) []interface{} {
	if r.deletes[k] {
		return nil
	}
	v, ok := r.updates[k]
	if ok {
		return v
	}
	return r.windows[k]
}

func (r *Row) Set(k window.Span, v []interface{}) {
	delete(r.deletes, k)
	r.updates[k] = v
}

func (r *Row) Windows() map[window.Span][]interface{} {
	ss := map[window.Span][]interface{}{}
	for k, v := range r.updates {
		ss[k] = v
	}
	for k, v := range r.windows {
		if r.deletes[k] {
			continue
		}
		if _, ok := r.updates[k]; ok {
			continue
		}
		ss[k] = v
	}
	return ss
}

func (r *Row) Flush() error {
	for k := range r.deletes {
		err := r.rw.DelSpan(k)
		if err != nil {
			return err
		}
	}
	for k, v := range r.updates {
		err := r.rw.PutSpan(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
