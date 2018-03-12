package window

import (
	"sync"
)

func newState() *state {
	return &state{
		Windows: map[Span][]interface{}{},
	}
}

// state of windows for a key.
type state struct {
	mu       sync.Mutex
	dataType string
	Windows  map[Span][]interface{}
}

// Get value for span s.
func (st *state) Get(s Span) []interface{} {
	return st.Windows[s]
}

// Coalesce span s by settings its value to vs, and
// removing span set rs.
func (st *state) Coalesce(s Span, rs []Span, vs []interface{}) {
	st.Windows[s] = vs
}

// Spans of the state.
func (st *state) Spans() []Span {
	snap := make([]Span, 0, len(st.Windows))
	for s := range st.Windows {
		snap = append(snap, s)
	}
	return snap
}

// Snapshot the state.
func (st *state) Snapshot() *state {
	n := newState()
	for k, v := range st.Windows {
		n.Windows[k] = v
	}
	return n
}

func appendMerge(a, b []interface{}) ([]interface{}, error) {
	c := append(a, b...)
	return c, nil
}

func item(i int) interface{} {
	return i
}

func equal(vs, ws []interface{}) bool {
	if len(vs) != len(ws) {
		return false
	}

	contains := func(v interface{}, bag []interface{}) bool {
		for _, b := range bag {
			if v.(int) == b.(int) {
				return true
			}
		}
		return false
	}

	for _, v := range vs {
		if !contains(v, ws) {
			return false
		}
	}
	return true
}
