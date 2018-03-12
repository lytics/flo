package bigtabledriver

import (
	"cloud.google.com/go/bigtable"
	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
)

func newRW(key string, tbl *bigtable.Table) *rw {
	return &rw{
		tbl:    tbl,
		mut:    bigtable.NewMutation(),
		prefix: key,
	}
}

type rw struct {
	tbl    *bigtable.Table
	mut    *bigtable.Mutation
	prefix string
}

func (rw *rw) Del(s window.Span) error {
	k, err := encodeKey(s)
	if err != nil {
		return err
	}
	rw.mut.DeleteCellsInColumn(windowFamily, k)
	return nil
}

func (rw *rw) Set(s window.Span, rec driver.Record) error {
	k, err := encodeKey(s)
	if err != nil {
		return err
	}
	v, err := encodeVal(rec)
	if err != nil {
		return err
	}
	rw.mut.Set(windowFamily, k, bigtable.ServerTime, v)
	return nil
}

func (rw *rw) Snapshot() (map[window.Span]driver.Record, error) {
	row, err := rw.tbl.ReadRow(nil, rw.prefix)
	if err != nil {
		return nil, err
	}
	snapTs := map[window.Span]bigtable.Timestamp{}
	snapVs := map[window.Span]driver.Record{}
	for _, item := range row[windowFamily] {
		k, err := decodeKey(item.Column)
		if err != nil {
			return nil, err
		}
		v, err := decodeVal(item.Value)
		if err != nil {
			return nil, err
		}
		if snapTs[k] < item.Timestamp {
			snapTs[k] = item.Timestamp
			snapVs[k] = *v
		}
	}
	return snapVs, nil
}

func (rw *rw) flush() error {
	return rw.tbl.Apply(nil, string(rw.prefix), rw.mut)
}
