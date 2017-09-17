package jsonfile

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"

	"github.com/lytics/flo/source"
)

// FromFile reads JSON objects from the file. Parameter
// prototype should be a non-pointer value of the type
// to decode into, for example:
//
//     data, err := jsonfile.FromFile(MyType{}, "event.data")
//
func FromFile(prototype interface{}, name string) *Source {
	return &Source{
		meta: source.Metadata{
			Name:       name,
			Addressing: source.Sequential,
		},
		prototype: prototype,
	}
}

// Source from JSON encoded values.
type Source struct {
	pos       int
	f         *os.File
	stream    *json.Decoder
	meta      source.Metadata
	prototype interface{}
}

// Metadata about source.
func (s *Source) Metadata() source.Metadata {
	return s.meta
}

// Init the source.
func (s *Source) Init() error {
	f, err := os.Open(s.meta.Name)
	if err != nil {
		return err
	}
	s.f = f
	s.stream = json.NewDecoder(bufio.NewReader(s.f))
	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	return s.f.Close()
}

// Take next value of the source.
func (s *Source) Take(ctx context.Context) (source.ID, interface{}, error) {
	select {
	case <-ctx.Done():
		return source.NoID, nil, context.DeadlineExceeded
	default:
	}

	v := reflect.New(reflect.TypeOf(s.prototype)).Interface()
	if err := s.stream.Decode(v); err != nil {
		return source.NoID, nil, err
	}
	id := source.ID(strconv.Itoa(s.pos))
	s.pos++

	return id, v, nil
}
