package badgerdriver

import (
	"github.com/dgraph-io/badger"
	"github.com/lytics/flo/window"
)

func newRW(key string, txn *badger.Txn) *rw {
	return &rw{
		txn:    txn,
		prefix: []byte(key),
	}
}

type rw struct {
	txn    *badger.Txn
	prefix []byte
}

func (rw *rw) DelSpan(s window.Span) error {
	k, err := encodeKey(s, rw)
	if err != nil {
		return err
	}
	return rw.txn.Delete(k)
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

	return rw.txn.Set(k, v, 0)
}

func (rw *rw) Windows() (map[window.Span][]interface{}, error) {
	it := rw.txn.NewIterator(badger.DefaultIteratorOptions)
	snap := map[window.Span][]interface{}{}
	for it.Seek(rw.prefix); it.ValidForPrefix(rw.prefix); it.Next() {
		item := it.Item()
		kb := item.Key()
		vb, err := item.Value()
		if err != nil {
			return nil, err
		}
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
