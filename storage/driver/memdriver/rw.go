package memdriver

import (
	"sync"

	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
)

func newRW(key string) *rw {
	return &rw{
		key:  key,
		data: map[window.Span]driver.Record{},
	}
}

type rw struct {
	mu   sync.Mutex
	key  string
	data map[window.Span]driver.Record
}

func (rw *rw) Del(s window.Span) {
	delete(rw.data, s)
}

func (rw *rw) Set(s window.Span, rec driver.Record) error {
	rw.data[s] = driver.Record{
		Clock:  rec.Clock,
		Count:  rec.Count,
		Values: rec.Values,
	}
	return nil
}

func (rw *rw) Snapshot() (map[window.Span]driver.Record, error) {
	snap := map[window.Span]driver.Record{}
	for k, rec := range rw.data {
		snap[k] = driver.Record{
			Count:  rec.Count,
			Clock:  rec.Clock,
			Values: rec.Values,
		}
	}
	return snap, nil
}
