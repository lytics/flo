package badgerdriver

import (
	"context"

	"github.com/dgraph-io/badger"
	"github.com/lytics/flo/storage"
	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
)

func init() {
	storage.Register("badger", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string, cfg driver.Cfg) (driver.Conn, error) {
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

func (c *Conn) Apply(ctx context.Context, key string, mut driver.Mutation) (map[window.Span]driver.Update, error) {
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
