package jsonfile

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
)

// Objects from JSON encoded values.
type Objects struct {
	pos       int
	f         *os.File
	stream    *json.Decoder
	filename  string
	prototype interface{}
}

// Name of the source.
func (s *Objects) Name() string {
	return s.filename
}

// Next value or io.EOF.
func (s *Objects) Next(context.Context) (string, interface{}, error) {
	for {
		v := reflect.New(reflect.TypeOf(s.prototype)).Interface()
		if err := s.stream.Decode(v); err != nil {
			return "", nil, err
		}
		pos := s.pos
		s.pos++
		return strconv.Itoa(pos), v, nil
	}
}

// Init the collection.
func (s *Objects) Init() error {
	f, err := os.Open(s.filename)
	if err != nil {
		return err
	}
	s.f = f
	s.stream = json.NewDecoder(bufio.NewReader(f))
	return nil
}

// Close the collection.
func (s *Objects) Close() error {
	return s.f.Close()
}

// FromObjects reads JSON objects from the file. Parameter
// prototype should be a non-pointer value of the type
// to decode into, for example:
//
//     objs, err := jsonfile.FromObjects(MyType{}, "event.data")
//
func FromObjects(prototype interface{}, filename string) *Objects {
	return &Objects{
		filename:  filename,
		prototype: prototype,
	}
}
