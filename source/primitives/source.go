package primitives

import (
	"context"
	"fmt"
	"io"

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
func (s *Source) Init(checkpoint interface{}) error {
	cp, ok := checkpoint.(*Checkpoint)
	if !ok {
		return fmt.Errorf("primities: checkpoint must be of type primities.Checkpoint, not: %T", checkpoint)
	}
	if cp.Name != s.meta.Name {
		return fmt.Errorf("primities: checkpoint name: %v, different from source name: %v", cp.Name, s.meta.Name)
	}
	s.pos = int(cp.Pos)

	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	return nil
}

// Take next item from source.
func (s *Source) Take(ctx context.Context) (*source.Item, error) {
	select {
	case <-ctx.Done():
		return nil, context.DeadlineExceeded
	default:
	}

	if s.pos >= len(s.data) {
		return nil, io.EOF
	}
	v := s.data[s.pos]
	item := source.NewItem(v, &Checkpoint{
		Name: s.meta.Name,
		Pos:  int64(s.pos),
	}, nil)

	s.pos++

	return item, nil
}
