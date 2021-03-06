package bigtabledriver

import (
	"context"

	"cloud.google.com/go/bigtable"
	"github.com/lytics/flo/storage"
	"github.com/lytics/flo/storage/driver"
)

func init() {
	storage.Register("bigtable", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string) (driver.Conn, error) {
	client, err := bigtable.NewClient(nil, "", "")
	if err != nil {
		return nil, err
	}
	return &Conn{
		table: client.Open(""),
	}, nil
}

type Conn struct {
	table *bigtable.Table
}

func (c *Conn) Apply(ctx context.Context, key string, mut driver.Mutation) error {
	rw := newRW(key, c.table)

	row, err := driver.NewRow(rw)
	if err != nil {
		return err
	}

	err = mut(row)
	if err != nil {
		return err
	}

	err = row.Flush()
	if err != nil {
		return err
	}

	return rw.flush()
}

func (c *Conn) Drain(ctx context.Context, keys []string, sink driver.Sink) error {
	return nil
}
