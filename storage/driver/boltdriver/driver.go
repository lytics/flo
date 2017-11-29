package boltdriver

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/boltdb/bolt"
	"github.com/lytics/flo/storage"
	"github.com/lytics/flo/storage/driver"
)

const (
	// DriverName used for driver registration.
	DriverName = "bolt"
)

func init() {
	storage.Register(DriverName, &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string, cfg driver.Cfg) (driver.Conn, error) {
	boltCfg, ok := cfg.(Cfg)
	if !ok {
		return nil, fmt.Errorf("boltdriver: unknown configuration type: %T", cfg)
	}

	// Location of database.
	loc := path.Join(boltCfg.BaseDir, name)

	// File mode of database.
	mod := os.FileMode(600)
	if boltCfg.FileMode > 0 {
		mod = boltCfg.FileMode
	}

	// Options for database.
	opt := bolt.DefaultOptions
	if boltCfg.Options != nil {
		opt = boltCfg.Options
	}

	db, err := bolt.Open(loc, mod, opt)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		db:     db,
		bucket: "default",
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(c.bucketKey())
		return err
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}

type Conn struct {
	db     *bolt.DB
	bucket string
}

func (c *Conn) Apply(ctx context.Context, key string, mut driver.Mutation) error {
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

func (c *Conn) Drain(ctx context.Context, keys []string, sink driver.Sink) error {
	return c.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket(c.bucketKey())

		for _, key := range keys {
			rw := newRW(key, bk)

			row, err := driver.NewRow(rw)
			if err != nil {
				return err
			}

			for s, vs := range row.Windows() {
				err := sink(ctx, s, key, vs)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (c *Conn) bucketKey() []byte {
	return []byte(c.bucket)
}
