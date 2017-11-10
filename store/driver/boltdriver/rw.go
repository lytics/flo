package boltdriver

import (
	"bytes"

	"github.com/boltdb/bolt"
	"github.com/lytics/flo/window"
)

func newRW(key string, b *bolt.Bucket) *rw {
	return &rw{
		b:      b,
		prefix: []byte(key),
	}
}

type rw struct {
	b      *bolt.Bucket
	prefix []byte
}

func (rw *rw) DelSpan(s window.Span) error {
	k, err := encodeKey(s, rw)
	if err != nil {
		return err
	}
	return rw.b.Delete(k)
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

	return rw.b.Put(k, v)
}

func (rw *rw) Windows() (map[window.Span][]interface{}, error) {
	c := rw.b.Cursor()
	snap := map[window.Span][]interface{}{}
	for kb, vb := c.Seek(rw.prefix); kb != nil && bytes.HasPrefix(kb, rw.prefix); kb, vb = c.Next() {
		k, err := decodeKey(kb, rw)
		if err != nil {
			return nil, err
		}
		v, err := decodeVal(vb)
		if err != nil {
			return nil, err
		}
		snap[k] = v
	}
	return snap, nil
}
