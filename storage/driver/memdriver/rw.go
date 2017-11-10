package memdriver

import (
	"sync"

	"github.com/lytics/flo/window"
)

func newRW(key string) *rw {
	return &rw{
		key: key,
		windows: map[window.Span][]interface{}{},
	}
}

type rw struct {
	mu sync.Mutex
	key string
	windows map[window.Span][]interface{}
}

func (rw *rw) DelSpan(s window.Span) error {
	delete(rw.windows, s)
	return nil
}

func (rw *rw) PutSpan(s window.Span, vs []interface{}) error {
	rw.windows[s] = vs
	return nil
}

func (rw *rw) Windows() (map[window.Span][]interface{}, error) {
	snap := map[window.Span][]interface{}{}
	for k, v := range rw.windows {
		snap[k] = v
	}
	return snap, nil
}
