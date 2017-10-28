package worker

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/internal/process/mapred"
	"github.com/lytics/flo/internal/registry"
	"github.com/lytics/flo/internal/schedule"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/grid"
	"golang.org/x/sync/errgroup"
)

type Open func(name string) (*txdb.DB, error)

type Define func(graphType string) (*graph.Definition, bool)

type Watch func(ctx context.Context) ([]*registry.WatchEvent, <-chan *registry.WatchEvent, error)

type Peers func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Mailboxes func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

func New(d Define, o Open, s Send, l Listen, w Watch, p Peers, m Mailboxes) (grid.Actor, error) {
	return &Actor{
		logger:    log.New(os.Stderr, "worker: ", log.LstdFlags),
		procs:     newProcesses(),
		timeout:   10 * time.Second,
		open:      o,
		define:    d,
		send:      s,
		listen:    l,
		watch:     w,
		peers:     p,
		mailboxes: m,
	}, nil
}

type Actor struct {
	eg      *errgroup.Group
	ctx     context.Context
	name    string
	procs   *procs
	logger  *log.Logger
	timeout time.Duration
	// Outside world
	open      Open
	define    Define
	watch     Watch
	peers     Peers
	mailboxes Mailboxes
	send      Send
	listen    Listen
}

func (a *Actor) Act(ctx context.Context) {
	defer a.logger.Print("exited")

	name, err := grid.ContextActorName(ctx)
	if err != nil {
		a.logger.Printf("failed getting name: %v", err)
	}
	a.ctx = ctx
	a.name = name
	a.logger = log.New(os.Stderr, name+": ", log.LstdFlags)
	a.logger.Print("running")

	a.eg, a.ctx = errgroup.WithContext(ctx)
	a.eg.Go(a.runTermWatcher)
	a.eg.Go(a.runGraphWatcher)
	err = a.eg.Wait()
	if err != nil {
		a.logger.Printf("failed with: %v", err)
	}
}

func (a *Actor) runTermWatcher() error {
	defer a.logger.Print("term watcher exited")
	a.logger.Print("term watcher running")

	propogate := func(v interface{}) {
		term, ok := v.(*msg.Term)
		if !ok {
			a.logger.Printf("unknonw response for term message: %T", v)
		} else {
			r, err := schedule.New(term.Peers)
			if err != nil {
				a.logger.Printf("failed creating ring from term: %v", err)
			} else {
				for _, p := range a.procs.AllRunning() {
					p.SetRing(r)
				}
			}
		}
	}

	timer := time.NewTimer(0 * time.Second)
	for {
		select {
		case <-a.ctx.Done():
			return nil
		case <-timer.C:
			res, err := a.send(a.timeout, "leader", &msg.Term{})
			if err != nil {
				a.logger.Printf("failed getting term: %v", err)
			} else {
				propogate(res)
			}
			timer.Reset(10 * time.Second)
		}
	}
}

func (a *Actor) runGraphWatcher() error {
	defer a.logger.Print("graph watcher exited")
	a.logger.Print("graph watcher running")

	current, events, err := a.watch(a.ctx)
	if err != nil {
		a.logger.Printf("failed watch: %v", err)
		return err
	}

	for _, e := range current {
		a.evalEvent(e)
	}

	defer func() {
		for _, p := range a.procs.AllRunning() {
			a.logger.Printf("stopping: %v", p)
			p.Stop()
		}
	}()

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case e, open := <-events:
			if !open {
				return nil
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
	_, ok = a.procs.Running(key)
	if ok {
		return
	}
	p := mapred.New(
		a.name,
		graphType,
		graphName,
		conf,
		def,
		mapred.Open(a.open),
		mapred.Send(a.send),
		mapred.Listen(a.listen),
	)
	a.procs.SetRunning(key, p)
	a.logger.Printf("starting graph: %v", p)
	go func() {
		err := p.Run()
		if err != nil {
			a.logger.Printf("graph: %v: failed: %v", p, err)
		}
	}()
}

func (a *Actor) stopGraph(key string) {
	p, ok := a.procs.Running(key)
	if ok {
		a.logger.Printf("stopping graph: %v", p)
		p.Stop()
	}
}

func (a *Actor) terminateGraph(key string) {
	p, ok := a.procs.Running(key)
	if ok {
		a.logger.Printf("terminating graph: %v", p)
		p.Stop()
	}
}

func newProcesses() *procs {
	return &procs{
		running: map[string]*mapred.Process{},
	}
}

type procs struct {
	mu      sync.Mutex
	running map[string]*mapred.Process
}

func (procs *procs) Running(key string) (*mapred.Process, bool) {
	procs.mu.Lock()
	defer procs.mu.Unlock()

	p, ok := procs.running[key]
	return p, ok
}

func (procs *procs) SetRunning(key string, p *mapred.Process) bool {
	procs.mu.Lock()
	defer procs.mu.Unlock()

	_, ok := procs.running[key]
	if !ok {
		procs.running[key] = p
		return true
	}
	return false
}

func (procs *procs) AllRunning() map[string]*mapred.Process {
	procs.mu.Lock()
	defer procs.mu.Unlock()

	running := map[string]*mapred.Process{}
	for k, v := range procs.running {
		running[k] = v
	}

	return running
}
