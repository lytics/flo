package bigtabledriver

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/lytics/flo/window"
	"github.com/lytics/grid/codec"
)

func encodeKey(s window.Span) (string, error) {
	sk, err := s.Key()
	return string(sk), err
}

func decodeKey(kb string) (window.Span, error) {
	return window.NewSpanFromKey([]byte(kb))
}

func encodeVal(vs []interface{}) ([]byte, error) {
	vec := &Vector{}
	dataType := ""
	for _, v := range vs {
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
			return nil, err
		}
		res = append(res, v)
	}

	return res, nil
}
