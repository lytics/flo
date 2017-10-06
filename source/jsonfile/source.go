package jsonfile

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"sync"

	"github.com/lytics/flo/source"
)

// New json file source. Parameter prototype should be a non-pointer
// value of the type to decode into, for example:
//
//     data, err := jsonfile.FromFile(MyType{}, "event.data")
//
func New(prototype interface{}, name string) *Source {
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
	mu        sync.Mutex
	pos       int
	f         *os.File
	stream    *json.Decoder
	meta      source.Metadata
	prototype interface{}
}

// Metadata about source.
func (s *Source) Metadata() source.Metadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.meta
}

// Init the source.
func (s *Source) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f != nil {
		return nil
	}

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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return nil
	}

	f := s.f
	s.f = nil

	return f.Close()
}

// Take next value of the source.
func (s *Source) Take(ctx context.Context) (source.ID, interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
