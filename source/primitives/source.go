package primitives

import (
	"context"
	"io"
	"strconv"

	"github.com/lytics/flo/source"
)

// FromStrings creates a finite collection of strings.
func FromStrings(name string, ss []string) *Source {
	return &Source{name: name, data: ss}
}

type Source struct {
	pos  int
	data []string
	name string
}

func (s *Source) Metadata() (*source.Metadata, error) {
	return &source.Metadata{
		Name:       s.name,
		Addressing: source.Sequential,
	}, nil
}

func (s *Source) Next(context.Context) (string, interface{}, error) {
	if s.pos >= len(s.data) {
		return "", nil, io.EOF
	}

	w := s.data[s.pos]
	pos := s.pos
	s.pos++

	return strconv.Itoa(pos), w, nil
}

func (s *Source) Init(pos string) error {
	if pos != "" {
		posNum, err := strconv.Atoi(pos)
		if err != nil {
			return err
		}
		for i := 0; i < posNum; i++ {
			s.Next(context.Background())
		}
	}
	return nil
}

func (s *Source) Stop() error {
	return nil
}
