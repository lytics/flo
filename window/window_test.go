package window

import (
	"sync"
	"time"
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

func (s *state) Del(k Span) {
	delete(s.Windows, k)
}

func (s *state) Get(k Span) []interface{} {
	return s.Windows[k]
}

func (s *state) Set(k Span, v []interface{}) {
	s.Windows[k] = v
}

func (s *state) Spans() []Span {
	ss := make([]Span, 0, len(s.Windows))
	for k := range s.Windows {
		ss = append(ss, k)
	}
	return ss
}

func (s *state) Snapshot() *state {
	n := newState()
	for k, v := range s.Windows {
		n.Windows[k] = v
	}
	return n
}

func appendMerge(a, b []interface{}) ([]interface{}, error) {
	c := append(a, b...)
	return c, nil
}

func items(i int) []interface{} {
	return []interface{}{i}
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

func clock(t time.Time) string {
	return t.Format("15:04:05")
}
