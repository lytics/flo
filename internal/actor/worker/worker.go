package worker

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/process/mapred"
	"github.com/lytics/flo/internal/registry"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/grid"
)

type Define func(graphType string) (*graph.Definition, bool)

type Watch func(ctx context.Context) ([]*registry.WatchEvent, <-chan *registry.WatchEvent, error)

type Peers func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Mailboxes func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

func New(db *txdb.DB, d Define, s Send, l Listen, w Watch, p Peers, m Mailboxes) (grid.Actor, error) {
	return &Actor{
		logger:    log.New(os.Stderr, "worker: ", log.LstdFlags),
		running:   map[string]*mapred.Process{},
		db:        db,
		define:    d,
		send:      s,
		listen:    l,
		watch:     w,
		peers:     p,
		mailboxes: m,
	}, nil
}

type Actor struct {
	name    string
	logger  *log.Logger
	running map[string]*mapred.Process
	// Outside world
	db        *txdb.DB
	define    Define
	watch     Watch
	peers     Peers
	mailboxes Mailboxes
	send      Send
	listen    Listen
}

func (a *Actor) Act(ctx context.Context) {
	defer a.logger.Printf("exiting")

	name, err := grid.ContextActorName(ctx)
	if err != nil {
		a.logger.Printf("failed getting name: %v", err)
	}
	a.name = name
	a.logger = log.New(os.Stderr, name+": ", log.LstdFlags)

	current, events, err := a.watch(ctx)
	if err != nil {
		a.logger.Printf("failed watch: %v", err)
		return
	}

	for _, e := range current {
		a.evalEvent(e)
	}

	defer func() {
		for _, p := range a.running {
			p.Stop()
		}
	}()

	for {
		select {
		case e, open := <-events:
			if !open {
				return
			}
			a.evalEvent(e)
		}
	}
}

func (a *Actor) evalEvent(e *registry.WatchEvent) {
	a.logger.Printf("got event: %v", e)

	if e == nil || e.Reg == nil {
		return
	}

	key := e.Reg.Type + "." + e.Reg.Name
	graphType := e.Reg.Type
	graphName := e.Reg.Name

	switch e.Reg.Wanted {
	case "running":
		conf, err := e.Reg.UnmarshalConfig()
		if err != nil {
			a.logger.Printf("for graph: %v, failed unmarshaling config: %v", key, err)
			return
		}
		a.runGraph(key, graphType, graphName, conf)
	case "stopping":
		a.stopGraph(key)
	case "terminating":
		a.terminateGraph(key)
	}
}

func (a *Actor) runGraph(key, graphType, graphName string, conf []byte) {
	def, ok := a.define(graphType)
	if !ok {
		return
	}
	_, ok = a.running[key]
	if ok {
		return
	}
	bucket := a.db.Bucket(key)
	p := mapred.New(
		a.name,
		graphType,
		graphName,
		conf,
		def,
		bucket,
		mapred.Send(a.send),
		mapred.Listen(a.listen),
	)
	a.running[key] = p
	a.logger.Printf("starting graph: %v", key)
	go func() {
		err := p.Run()
		if err != nil {
			a.logger.Printf("graph: %v: failed: %v", key, err)
		}
	}()
}

func (a *Actor) stopGraph(key string) {
	p, ok := a.running[key]
	if ok {
		a.logger.Printf("stopping graph: %v", key)
		p.Stop()
	}
}

func (a *Actor) terminateGraph(key string) {
	p, ok := a.running[key]
	if ok {
		a.logger.Printf("terminating graph: %v", key)
		p.Stop()
	}
}
