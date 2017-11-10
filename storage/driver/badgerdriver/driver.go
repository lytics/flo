package badgerdriver

import (
	"github.com/dgraph-io/badger"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/flo/internal/txdb/driver"
	"github.com/lytics/flo/window"
)

func init() {
	txdb.Register("badger", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string) (driver.Conn, error) {
	db, err := badger.Open(badger.Options{Dir: name, ValueDir: name})
	if err != nil {
		return nil, err
	}
	return &Conn{
		db: db,
	}, nil
}

type Conn struct {
	db *badger.DB
}

func (c *Conn) Apply(key string, mut func(window.State) error) error {
	return c.db.Update(func(txn *badger.Txn) error {
		rw := newRW(key, txn)

		row, err := driver.NewRow(rw)
		if err != nil {
			return err
		}

		err = mut(row)
		if err != nil {
			return err
		}

		return row.Flush()
	})
}

func (c *Conn) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {

}
