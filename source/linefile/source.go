package linefile

import (
	"bufio"
	"context"
	"os"
	"strconv"

	"github.com/lytics/flo/source"
)

// FromFile create a source.
func FromFile(name string) *Source {
	return &Source{
		meta: source.Metadata{
			Name:       name,
			Addressing: source.Sequential,
		},
	}
}

// Source of data.
type Source struct {
	f    *os.File
	r    *bufio.Reader
	pos  int
	meta source.Metadata
}

// Metadata about the source.
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
	s.r = bufio.NewReader(s.f)
	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	return s.f.Close()
}

// Take next value from source.
func (s *Source) Take(ctx context.Context) (source.ID, interface{}, error) {
	select {
	case <-ctx.Done():
		return source.NoID, nil, context.DeadlineExceeded
	default:
	}

	v, err := s.r.ReadString('\n')
	if err != nil {
		return source.NoID, nil, err
	}
	id := source.ID(strconv.Itoa(s.pos))
	s.pos++

	return id, v, nil
}
