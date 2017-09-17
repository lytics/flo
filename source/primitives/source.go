package primitives

import (
	"context"
	"io"
	"strconv"

	"github.com/lytics/flo/source"
)

// FromSlice creates a finite collection of data.
func FromSlice(name string, data []interface{}) *Source {
	return &Source{
		meta: source.Metadata{
			Name:       name,
			Addressing: source.Sequential,
		},
		data: data,
	}
}

// Source of data.
type Source struct {
	pos  int
	data []interface{}
	meta source.Metadata
}

// Metadata about the source.
func (s *Source) Metadata() source.Metadata {
	return s.meta
}

// Init the source.
func (s *Source) Init() error {
	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	return nil
}

// Take next item from source.
func (s *Source) Take(ctx context.Context) (source.ID, interface{}, error) {
	select {
	case <-ctx.Done():
		return source.NoID, nil, context.DeadlineExceeded
	default:
	}

	if s.pos >= len(s.data) {
		return source.NoID, nil, io.EOF
	}
	v := s.data[s.pos]
	id := source.ID(strconv.Itoa(s.pos))
	s.pos++

	return id, v, nil
}
