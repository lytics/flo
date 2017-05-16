package primitives

import (
	"context"
	"io"
	"strconv"
)

type Strings struct {
	pos  int
	data []string
	name string
}

func (s *Strings) Name() string {
	return s.name
}

func (s *Strings) Next(context.Context) (string, interface{}, error) {
	if s.pos >= len(s.data) {
		return "", nil, io.EOF
	}

	w := s.data[s.pos]
	pos := s.pos
	s.pos++

	return strconv.Itoa(pos), w, nil
}

func (s *Strings) Init() error {
	return nil
}

func (s *Strings) Close() error {
	return nil
}

// FromStrings creates a finite collection of strings.
func FromStrings(name string, ss []string) *Strings {
	return &Strings{name: name, data: ss}
}
