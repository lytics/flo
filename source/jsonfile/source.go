package jsonfile

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/lytics/flo/source"
	"github.com/lytics/flo/source/metadatascanner"
)

// FromFile reads JSON objects from the file. Parameter
// prototype should be a non-pointer value of the type
// to decode into, for example:
//
//     data, err := jsonfile.FromFile(MyType{}, "event.data")
//
func FromFile(prototype interface{}, name string) *Source {
	return &Source{
		name:      name,
		prototype: prototype,
	}
}

func FromFiles(prototype interface{}, glob string) []*Source {
	return nil
}

// Source from JSON encoded values.
type Source struct {
	pos       int
	f         *os.File
	stream    *json.Decoder
	name      string
	prototype interface{}
	h         *source.Metadata
	inspect   func(interface{}) (time.Time, error)
}

// WithMeta will cause the file to be read fully to discover
// timestamp and size metadata of the file contents.
func (s *Source) WithMeta(f func(interface{}) (time.Time, error)) *Source {
	s.inspect = f
	return s
}

// Metadata about source.
func (s *Source) Metadata() (*source.Metadata, error) {
	var meta *source.Metadata
	var err error
	if s.inspect != nil {
		err = s.Init("")
		if err != nil {
			return nil, err
		}
		defer s.Stop()
		meta, err = metadatascanner.Scan(s, s.inspect)
		if err != nil {
			return nil, err
		}
	} else {
		meta = &source.Metadata{}
	}
	meta.Name = s.name
	meta.Addressing = source.Sequential
	return meta, nil
}

// Next value of the source.
func (s *Source) Next(context.Context) (source.ID, interface{}, error) {
	for {
		v := reflect.New(reflect.TypeOf(s.prototype)).Interface()
		if err := s.stream.Decode(v); err != nil {
			return "", nil, err
		}
		pos := s.pos
		s.pos++
		id := source.ID(strconv.Itoa(pos))
		return id, v, nil
	}
}

// Init the source.
func (s *Source) Init(pos source.ID) error {
	f, err := os.Open(s.name)
	if err != nil {
		return err
	}
	s.f = f
	s.stream = json.NewDecoder(bufio.NewReader(s.f))
	if pos != "" {
		posNum, err := strconv.Atoi(string(pos))
		if err != nil {
			return err
		}
		for i := 0; i < posNum; i++ {
			s.Next(context.Background())
		}
	}
	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	return s.f.Close()
}
