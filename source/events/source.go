package events

import (
	"context"
	"io"
	"time"

	"strconv"

	"github.com/lytics/flo/source"
	"github.com/lytics/flo/source/metainspector"
)

type Event struct {
	ID  string
	TS  time.Time
	Msg interface{}
}

func New(name string, events []Event) *Source {
	return &Source{
		name:   name,
		events: events,
	}
}

type Source struct {
	h      source.Metadata
	pos    int
	name   string
	events []Event
}

func (s *Source) Metadata() (*source.Metadata, error) {
	meta, err := metainspector.Inspect(s, func(v interface{}) (time.Time, error) {
		e := v.(Event)
		return e.TS, nil
	})
	if err != nil {
		return nil, err
	}
	meta.Name = s.name
	meta.Addressing = source.Sequential
	return meta, nil
}

func (s *Source) Next(context.Context) (source.ID, interface{}, error) {
	if s.pos == len(s.events) {
		return "", nil, io.EOF
	}
	e := s.events[s.pos]
	s.pos++
	return source.ID(e.ID), e.Msg, nil
}

func (s *Source) Init(pos source.ID) error {
	posNum, err := strconv.Atoi(string(pos))
	if err != nil {
		return err
	}
	s.pos = posNum
	return nil
}

func (s *Source) Stop() error {
	return nil
}
