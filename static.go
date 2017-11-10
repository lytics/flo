package flo

import (
	"sync"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/storage/driver"
)

// Cfg where the only required parameter is the namespace.
type Cfg struct {
	Driver    driver.Cfg
	Namespace string
}

var (
	graphsMu sync.Mutex
	graphs   map[string]*graph.Definition
)

func init() {
	graphs = map[string]*graph.Definition{}
}

// RegisterMsg where v is a non-pointer protobuf message type.
func RegisterMsg(v interface{}) error {
	return codec.Register(v)
}

// RegisterGraph of the given graph type.
func RegisterGraph(graphType string, g *graph.Graph) error {
	graphsMu.Lock()
	defer graphsMu.Unlock()

	if graphType == "" {
		return ErrInvalidGraphType
	}

	if g == nil {
		return ErrNilGraph
	}

	_, ok := graphs[graphType]
	if ok {
		return ErrAlreadyDefined
	}
	graphs[graphType] = g.Definition()
	return nil
}

// LookupGraph definition that was previously registered.
func LookupGraph(graphType string) (*graph.Definition, bool) {
	graphsMu.Lock()
	defer graphsMu.Unlock()

	def, ok := graphs[graphType]
	return def, ok
}
