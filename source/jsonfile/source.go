package jsonfile

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
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
func (s *Source) Init(checkpoint interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp, ok := checkpoint.(*Checkpoint)
	if !ok {
		return fmt.Errorf("jsonfile: checkpoint must be of type jsonfile.Checkpoint, not: %T", checkpoint)
	}
	if cp.Name != s.meta.Name {
		return fmt.Errorf("jsonfile: checkpoint name: %v, different from source name: %v", cp.Name, s.meta.Name)
	}

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
func (s *Source) Take(ctx context.Context) (*source.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, context.DeadlineExceeded
	default:
	}

	v := reflect.New(reflect.TypeOf(s.prototype)).Interface()
	if err := s.stream.Decode(v); err != nil {
		return nil, err
	}

	item := source.NewItem(v, &Checkpoint{
		Name: s.meta.Name,
		Pos:  int64(s.pos),
	}, nil)

	s.pos++

	return item, nil
}
