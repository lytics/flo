package linefile

import (
	"bufio"
	"context"
	"os"
	"strconv"

	"github.com/lytics/flo/source"
)

func FromFile(name string) *Source {
	return &Source{
		name: name,
	}
}

type Source struct {
	f    *os.File
	r    *bufio.Reader
	pos  int
	name string
}

func (f *Source) Metadata() (*source.Metadata, error) {
	return &source.Metadata{
		Name:       f.name,
		Addressing: source.Sequential,
	}, nil
}

func (s *Source) Next(context.Context) (source.ID, interface{}, error) {
	l, err := s.r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	pos := s.pos
	s.pos++
	id := source.ID(strconv.Itoa(pos))
	return id, l, nil
}

func (s *Source) Init(pos source.ID) error {
	f, err := os.Open(s.name)
	if err != nil {
		return err
	}
	s.f = f
	s.r = bufio.NewReader(s.f)
	if pos != "" {
		posNum, err := strconv.Atoi(string(pos))
		if err != nil {
			return err
		}
		for i := 0; i < posNum; i++ {
			if _, _, err := s.Next(nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Source) Stop() error {
	return s.f.Close()
}
