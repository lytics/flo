package boltdriver

import (
	"cloud.google.com/go/bigtable"
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

func (rw *rw) DelSpan(s window.Span) error {
	k, err := encodeKey(s, rw)
	if err != nil {
		return err
	}
	rw.mut.DeleteCellsInColumn("window", s.String())
	return nil
}

func (rw *rw) PutSpan(s window.Span, vs []interface{}) error {
	k, err := encodeKey(s, rw)
	if err != nil {
		return err
	}
	v, err := encodeVal(vs)
	if err != nil {
		return err
	}
	rw.mut.Set("window", k, bigtable.ServerTime, v)
	return nil
}

func (rw *rw) Windows() (map[window.Span][]interface{}, error) {
	row, err := rw.tbl.ReadRow(nil, rw.prefix)
	if err != nil {
		return nil, err
	}
	ts := map[window.Span]bigtable.Timestamp{}
	snap := map[window.Span][]interface{}{}
	for _, item := range row["window"] {
		k, err := decodeKey(item.Column, rw)
		if err != nil {
			return nil, err
		}
		v, err := decodeVal(item.Value)
		if err != nil {
			return nil, err
		}
		if ts[k] < item.Timestamp {
			ts[k] = item.Timestamp
			snap[k] = v
		}
	}
	return snap, nil
}

func (rw *rw) flush() error {
	return rw.tbl.Apply(nil, string(rw.prefix), rw.mut)
}
