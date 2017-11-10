package funcsink

import (
	"context"

	"github.com/lytics/flo/window"
)

func New(f func(ctx context.Context, w window.Span, key string, vs []interface{}) error) *Sink {
	return &Sink{
		f: f,
	}
}

type Sink struct {
	f func(ctx context.Context, w window.Span, key string, vs []interface{}) error
}

func (s *Sink) Init() error {
	return nil
}

func (s *Sink) Stop() error {
	return nil
}

func (s *Sink) Give(ctx context.Context, w window.Span, key string, vs []interface{}) error {
	return s.f(ctx, w, key, vs)
}
