package boltdriver

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/window"
)

func init() {
	codec.Register(Vector{})
}

func encodeKey(s window.Span, rw *rw) ([]byte, error) {
	sk, err := s.Key()
	if err != nil {
		return nil, err
	}

	sl := len(sk)
	pl := len(rw.prefix)
	fl := pl + 1 + sl

	fk := make([]byte, fl)

	// Expected format: <prefix>@<span>
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

func encodeVal(vs []interface{}) ([]byte, error) {
	vec := &Vector{}
	dataType := ""
	for _, v := range vs {
		datumType, datum, err := codec.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("boltdriver: failed to encode: %v", err)
		}
		if dataType == "" {
			dataType = datumType
		}
		if dataType != datumType {
			return nil, fmt.Errorf("boltdriver: invalid encoded data type: %v, expected: %v", datumType, dataType)
		}
		vec.Data = append(vec.Data, datum)
	}
	vec.DataType = dataType

	return proto.Marshal(vec)
}

func decodeVal(vb []byte) ([]interface{}, error) {
	vec := &Vector{}
	err := proto.Unmarshal(vb, vec)
	if err != nil {
		return nil, err
	}

	res := []interface{}{}
	for _, e := range vec.Data {
		v, err := codec.Unmarshal(e, vec.DataType)
		if err != nil {
			return nil, fmt.Errorf("boltdriver: failed to decode: %v", err)
		}
		res = append(res, v)
	}

	return res, nil
}
