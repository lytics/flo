package badgerdriver

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
	"github.com/lytics/grid/codec"
)

func encodeKey(s window.Span, rw *rw) ([]byte, error) {
	sk, err := s.Key()
	if err != nil {
		return nil, err
	}

	sl := len(sk)
	pl := len(rw.prefix)
	fl := pl + 1 + sl

	fk := make([]byte, fl)

	// Expected format: <previx>@<span>
	copy(fk[0:], rw.prefix)
	fk[pl] = '@'
	copy(fk[pl+1:], sk)

	return fk, nil
}

func decodeKey(kb []byte, rw *rw) (window.Span, error) {
	pl := len(rw.prefix)

	// Expected format: <prefix>@<span>
	if kb[pl] != '@' {
		return window.Span{}, fmt.Errorf("invalid key: %x", kb)
	}

	return window.NewSpanFromKey(kb[pl+1:])
}

func encodeVal(rec driver.Record) ([]byte, error) {
	vec := &Vector{
		Clock: rec.Clock,
		Count: rec.Count,
	}
	dataType := ""
	for _, v := range rec.Values {
		datumType, datum, err := codec.Marshal(v)
		if err != nil {
			return nil, err
		}
		if dataType == "" {
			dataType = datumType
		}
		if dataType != datumType {
			return nil, fmt.Errorf("invalid encoded data type: %v, expected: %v", datumType, dataType)
		}
		vec.Data = append(vec.Data, datum)
	}
	vec.DataType = dataType

	return proto.Marshal(vec)
}

func decodeVal(vb []byte) (*driver.Record, error) {
	vec := &Vector{}
	err := proto.Unmarshal(vb, vec)
	if err != nil {
		return nil, err
	}

	values := []interface{}{}
	for _, e := range vec.Data {
		v, err := codec.Unmarshal(e, vec.DataType)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	return &driver.Record{
		Clock:  vec.Clock,
		Count:  vec.Count,
		Values: values,
	}, nil
}
