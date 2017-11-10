package memdriver

import (
	"sync"

	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/flo/internal/txdb/driver"
	"github.com/lytics/flo/window"
)

func init() {
	txdb.Register("mem", &drvr{})
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

func (c *Conn) Apply(key string, mut func(window.State) error) error {
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

	return mut(row)
}

func (c *Conn) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {

}
