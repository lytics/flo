package graph

import (
	"time"

	"github.com/lytics/flo/merger"
	"github.com/lytics/flo/source"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

type Event struct {
	TS  time.Time
	ID  string
	Msg interface{}
}

type KeyedEvent struct {
	TS  time.Time
	Key string
	Msg interface{}
}

type Mapper interface {
	Transform(v interface{}) ([]Event, error)
	GroupAndWindowBy(id string, ts time.Time, v interface{}) ([]KeyedEvent, error)
}

type Reducer interface {
	Reduce(m KeyedEvent) error
}

func New(name string) *Graph {
	return &Graph{
		name:   name,
		window: window.All(),
	}
}

type Graph struct {
	name      string
	from      []source.Source
	transform func(interface{}) ([]Event, error)
	groupBy   func(interface{}) (string, error)
	merger    merger.Merger
	window    window.Window
	trigger   trigger.Trigger
	into      func(window.Span, string, []interface{}) error
}

func (g *Graph) From(vss ...source.Source) {
	g.from = vss
}

func (g *Graph) Transform(f func(interface{}) ([]Event, error)) {
	g.transform = f
}

func (g *Graph) GroupBy(f func(interface{}) (string, error)) {
	g.groupBy = f
}

func (g *Graph) Window(w window.Window) {
	g.window = w
}

func (g *Graph) Merger(f merger.Merger) {
	g.merger = f
}

func (g *Graph) Trigger(t trigger.Trigger) {
	g.trigger = t
}

func (g *Graph) Into(f func(window.Span, string, []interface{}) error) {
	g.into = f
}

func (g *Graph) Name() string {
	return g.name
}

func (g *Graph) Definition() *Definition {
	return &Definition{g}
}

type Definition struct {
	g *Graph
}

func (def *Definition) From() []source.Source {
	return def.g.from
}

func (def *Definition) Transform(v interface{}) ([]Event, error) {
	return def.g.transform(v)
}

func (def *Definition) GroupAndWindowBy(id string, ts time.Time, v interface{}) ([]KeyedEvent, error) {
	if def.g.groupBy != nil {
		key, err := def.g.groupBy(v)
		if err != nil {
			return nil, err
		}
		return []KeyedEvent{KeyedEvent{
			TS:  ts,
			Key: key,
			Msg: v,
		}}, nil
	} else if def.g.window != nil {
		var events []KeyedEvent
		windows := def.g.window.Apply(ts)
		for _, w := range windows {
			key := w.String()
			events = append(events, KeyedEvent{
				TS:  ts,
				Key: key,
				Msg: v,
			})
		}
		return events, nil
	} else {
		key := id
		return []KeyedEvent{KeyedEvent{
			TS:  ts,
			Key: key,
			Msg: v,
		}}, nil
	}
}

func (def *Definition) Merge(ke *KeyedEvent, windows map[window.Span][]interface{}) error {
	f := merger.Cons()
	if def.g.merger != nil {
		f = merger.Fold(def.g.merger)
	}
	return def.g.window.Merge(ke.TS, []interface{}{ke.Msg}, windows, f)
}

func (def *Definition) Trigger() trigger.Trigger {
	return def.g.trigger
}

func (def *Definition) Into() func(window.Span, string, []interface{}) error {
	return def.g.into
}

func (def *Definition) Name() string {
	return def.g.name
}
