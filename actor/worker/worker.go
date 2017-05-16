package worker

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/lytics/flo/codec"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/msg"
	"github.com/lytics/grid/grid.v3"
)

type Graphs interface {
	Lookup(name string) graph.Reducer
}

func New(c *grid.Client, s *grid.Server, gs Graphs) (grid.Actor, error) {
	return &Actor{
		graphs: gs,
		client: c,
		server: s,
		logger: log.New(os.Stderr, "worker: ", log.LstdFlags),
	}, nil
}

type Trigger struct {
	TS    time.Time
	Graph string
}

type Actor struct {
	graphs Graphs
	client *grid.Client
	server *grid.Server
	logger *log.Logger
}

func (a *Actor) Act(ctx context.Context) {
	name, err := grid.ContextActorName(ctx)
	if err != nil {
	}

	mailbox, err := grid.NewMailbox(a.server, name, 1000)
	if err != nil {
	}
	defer mailbox.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case req := <-mailbox.C:
			switch m := req.Msg().(type) {
			case *msg.Keyed:
				g := a.graphs.Lookup(m.Graph)
				if g == nil {
					a.logger.Printf("receive failed for undefined graph: %v", m.Graph)
				}

				v, err := codec.Unmarshal(m.Data, m.DataType)
				if err != nil {
					req.Respond(err)
					continue
				}

				err = g.Reduce(graph.KeyedEvent{
					TS:  m.TS(),
					Key: m.Key,
					Msg: v,
				})
				if err != nil {
					req.Respond(err)
					continue
				}
				req.Ack()
			}
		}
	}
}
