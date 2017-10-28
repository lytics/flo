package boltdriver

import (
	"cloud.google.com/go/bigtable"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/flo/internal/txdb/driver"
	"github.com/lytics/flo/window"
)

func init() {
	txdb.Register("bolt", &drvr{})
}

type drvr struct{}

func (d *drvr) Open(name string) (driver.Conn, error) {
	client, err := bigtable.NewClient(nil, "", "")
	if err != nil {
		return nil, err
	}
	return &Conn{
		table:  client.Open(""),
		bucket: "default",
	}, nil
}

type Conn struct {
	table  *bigtable.Table
	bucket string
}

func (c *Conn) Apply(key string, mut func(window.State) error) error {
	rw := newRW(key, c.table)

	row, err := driver.NewRow(rw)
	if err != nil {
		return err
	}

	err = mut(row)
	if err != nil {
		return err
	}

	row.Flush()
	return rw.flush()
}

func (c *Conn) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {

}

func (c *Conn) bucketKey() []byte {
	return []byte(c.bucket)
}
