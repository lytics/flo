package boltdriver

import (
	"github.com/boltdb/bolt"
	"github.com/lytics/flo/storage"
	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
)

func init() {
	storage.Register("bolt", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string) (driver.Conn, error) {
	db, err := bolt.Open(name, 600, bolt.DefaultOptions)
	if err != nil {
		return nil, err
	}
	return &Conn{
		db:     db,
		bucket: "default",
	}, nil
}

type Conn struct {
	db     *bolt.DB
	bucket string
}

func (c *Conn) Apply(key string, mut func(window.State) error) error {
	return c.db.Batch(func(tx *bolt.Tx) error {
		bk := tx.Bucket(c.bucketKey())
		rw := newRW(key, bk)

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

func (c *Conn) Drain(keys []string, sink driver.Sink) error {
	return nil
}

func (c *Conn) bucketKey() []byte {
	return []byte(c.bucket)
}
