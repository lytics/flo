package flo

import "errors"

var (
	// ErrNilGraph when a nil graph is defined.
	ErrNilGraph = errors.New("nil graph")
	// ErrAlreadyDefined when a graph is defined more than once.
	ErrAlreadyDefined = errors.New("already defined")
	// ErrInvalidGraphType when the graph type contains invalid
	// characters or is the empty string.
	ErrInvalidGraphType = errors.New("invalid graph type")
	// ErrInvalidNamespace when invalid characters appear in the namespace
	// or the namespace is the empty string.
	ErrInvalidNamespace = errors.New("invalid namespace")
)
