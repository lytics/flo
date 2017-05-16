package flo

import (
	"context"

	"github.com/lytics/flo/codec"
)

type Collection interface {
	Next(context.Context) (string, interface{}, error)
	Name() string
	Init() error
	Close() error
}

func Register(v interface{}) error {
	return codec.Register(v)
}
