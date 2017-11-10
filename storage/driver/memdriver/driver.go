package memdriver

import (
	"context"
	"sync"

	"github.com/lytics/flo/storage"
	"github.com/lytics/flo/storage/driver"
)

func init() {
	storage.Register("mem", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string) (driver.Conn, error) {
	return &Conn{
		name: name,
		data: map[string]*rw{},
	}, nil
}

type Conn struct {
	mu   sync.Mutex
	name string
	data map[string]*rw
}

func (c *Conn) Apply(ctx context.Context, key string, mut driver.Mutation) error {
	c.mu.Lock()
	rw, ok := c.data[key]
	if !ok {
		rw = newRW(key)
		c.data[key] = rw
	}
	c.mu.Unlock()

	rw.mu.Lock()
	defer rw.mu.Unlock()

	row, err := driver.NewRow(rw)
	if err != nil {
		return err
	}

	err = mut(row)
	if err != nil {
		return err
	}

	return row.Flush()
}

func (c *Conn) Drain(ctx context.Context, keys []string, sink driver.Sink) error {
	snap := map[string]*rw{}

	c.mu.Lock()
	for _, k := range keys {
		rw, ok := c.data[k]
		if ok {
			snap[k] = rw
		}
	}
	c.mu.Unlock()

	flush := func(rw *rw) error {
		rw.mu.Lock()
		defer rw.mu.Unlock()

		for s, vs := range rw.windows {
			err := sink(ctx, s, rw.key, vs)
			if err != nil {
				return err
			}
		}

		return nil
	}

	for _, rw := range snap {
		err := flush(rw)
		if err != nil {
			return err
		}
	}

	return nil
}
