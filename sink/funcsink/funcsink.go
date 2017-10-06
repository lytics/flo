package funcsink

import "github.com/lytics/flo/window"

func New(f func(w window.Span, key string, vs []interface{}) error) *Sink {
	return &Sink{
		f: f,
	}
}

type Sink struct {
	f func(w window.Span, key string, vs []interface{}) error
}

func (s *Sink) Init() error {
	return nil
}

func (s *Sink) Stop() error {
	return nil
}

func (s *Sink) Give(w window.Span, key string, vs []interface{}) error {
	return s.f(w, key, vs)
}
