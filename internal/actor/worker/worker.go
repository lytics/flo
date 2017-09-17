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
		a.runGraph(e)
	}

	for {
		select {
		case e := <-events:
			a.runGraph(e)
		}
	}
}

func (a *Actor) runGraph(e *registry.WatchEvent) {
	def, ok := a.define(e.Reg.Type)
	if ok {
		key := e.Reg.Type + ":" + e.Reg.Name
		_, ok := a.running[key]
		if !ok && e.Reg.Wanted == "running" {
			conf, err := e.Reg.UnmarshalConfig()
			if err != nil {
				a.logger.Printf("failed starting: %v.%v, failed to unmarshal config: %v", e.Reg.Type, e.Reg.Name, err)
			}
			bucket := a.db.Bucket(key)
			p := mapred.New(
				a.name,
				e.Reg.Type,
				e.Reg.Name,
				conf,
				def,
				bucket,
				mapred.Send(a.send),
				mapred.Listen(a.listen),
			)
			a.running[key] = p
			a.logger.Printf("starting graph: %v.%v", e.Reg.Type, e.Reg.Name)
			p.Run()
		}
	}
}
