package mapper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/process"
)

type PutMsg func(key string, ts time.Time, v interface{}) error

type PutError func(detail string, err error)

type Process struct {
	def      *graph.Definition
	tasks    <-chan *process.Task
	putMsg   PutMsg
	putError PutError
}

func New(e PutError, m PutMsg, def *graph.Definition, tasks <-chan *process.Task) *Process {
	return &Process{
		def:      def,
		tasks:    tasks,
		putMsg:   m,
		putError: e,
	}
}

func (p *Process) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case v, open := <-p.tasks:
			if v == nil {
				fmt.Println("the end", open)
			}
			events, err := p.def.Transform(v.Msg)
			if err != nil {
				p.putError("transform", err)
				return
			}
			keyed, err := groupAndWindow(p.def, events)
			if err != nil {
				p.putError("group-and-window", err)
				return
			}
			for _, ke := range keyed {
				err := p.putMsg(ke.Key, ke.TS, ke.Msg)
				if err != nil {
					p.putError("put-msg", err)
					return
				}
			}
		}
	}
}

func groupAndWindow(def graph.Mapper, events []graph.Event) ([]graph.KeyedEvent, error) {
	var keyed []graph.KeyedEvent
	for _, e := range events {
		grouped, err := def.GroupAndWindowBy(e.ID, e.TS, e.Msg)
		if err != nil {
			return nil, err
		}
		keyed = append(keyed, grouped...)
	}
	return keyed, nil
}
