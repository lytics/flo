package leader

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/internal/peerqueue"
	"github.com/lytics/flo/internal/registry"
	"github.com/lytics/flo/storage"
	"github.com/lytics/grid"
	"golang.org/x/sync/errgroup"
)

type Define func(graphType string) (*graph.Definition, bool)

type Watch func(ctx context.Context) ([]*registry.WatchEvent, <-chan *registry.WatchEvent, error)

type Peers func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Mailboxes func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

// New peer watcher.
func New(d Define, s Send, l Listen, w Watch, p Peers, m Mailboxes) (*Actor, error) {
	return &Actor{
		logger:    log.New(os.Stderr, "leader: ", log.LstdFlags),
		tracker:   peerqueue.New(),
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
	logger  *log.Logger
	tracker *peerqueue.PeerQueue
	// Outside world
	db        *storage.DB
	define    Define
	watch     Watch
	peers     Peers
	mailboxes Mailboxes
	send      Send
	listen    Listen
}

// String description of the actor.
func (a *Actor) String() string {
	return a.name
}

// Act as leader.
func (a *Actor) Act(ctx context.Context) {
	defer a.logger.Print("exited")

	name, err := grid.ContextActorName(ctx)
	if err != nil {
		a.logger.Printf("failed getting name: %v", err)
		return
	}
	a.name = name
	a.logger = log.New(os.Stderr, name+": ", log.LstdFlags)
	a.logger.Print("running")

	a.eg, a.ctx = errgroup.WithContext(ctx)
	a.eg.Go(a.runPeerWatcher)
	a.eg.Go(a.runTermWatcher)
	a.eg.Go(a.runWorkerWatcher)
	a.eg.Go(a.runMailboxWatcher)
	err = a.eg.Wait()
	if err != nil {
		a.logger.Printf("failed with: %v", err)
	}
}

func (a *Actor) runPeerWatcher() error {
	defer a.logger.Print("peer watcher exited")
	a.logger.Print("peer watcher running")

	current, peers, err := a.peers(a.ctx)
	if err != nil {
		return err
	}

	for _, c := range current {
		def := workerDef(c.Peer())
		a.logger.Printf("peer watcher: setting required: %v", def.Name)
		a.tracker.Live(c.Peer())
		a.tracker.SetRequired(def)
	}

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case e := <-peers:
			switch e.Type {
			case grid.WatchError:
				return e.Err()
			case grid.EntityLost:
				a.tracker.Dead(e.Peer())
				a.tracker.UnsetRequired(e.Peer())
			case grid.EntityFound:
				def := workerDef(e.Peer())
				a.logger.Printf("peer watcher: setting required: %v", def.Name)
				a.tracker.Live(e.Peer())
				a.tracker.SetRequired(def)
			}
		}
	}
}

func (a *Actor) runWorkerWatcher() error {
	defer a.logger.Print("worker watcher exited")
	a.logger.Print("worker watcher running")

	missingTimer := time.NewTimer(1 * time.Second)
	defer missingTimer.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case <-missingTimer.C:
			for _, def := range a.tracker.Missing() {
				err := a.startActor(def)
				if err != nil {
					a.logger.Printf("worker watcher: failed to start actor: %v, error: %v", def.Name, err)
				} else {
					a.logger.Printf("worker watcher: starting actor: %v", def.Name)
				}
			}
			missingTimer.Reset(1 * time.Second)
		}
	}
}

func (a *Actor) runMailboxWatcher() error {
	defer a.logger.Print("mailbox watcher exited")
	a.logger.Print("mailbox watcher running")

	current, mailboxes, err := a.mailboxes(a.ctx)
	if err != nil {
		return err
	}
	for _, c := range current {
		a.tracker.Register(c.Name(), c.Peer())
	}

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case e := <-mailboxes:
			switch e.Type {
			case grid.WatchError:
				return err
			case grid.EntityLost:
				a.tracker.Unregister(e.Name())
			case grid.EntityFound:
				a.tracker.Register(e.Name(), e.Peer())
			}
		}
	}
}

func (a *Actor) runTermWatcher() error {
	defer a.logger.Print("term watcher exited")
	a.logger.Print("term watcher running")

	var term []string
	for {
		a.logger.Printf("term watcher: waiting for peers to join")
		<-time.After(30 * time.Second)
		for peer := range a.tracker.Peers() {
			term = append(term, peer)
		}
		if len(term) > 0 {
			break
		}
	}

	events, close, err := a.listen(a.name)
	if err != nil {
		return err
	}
	defer close()

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case req := <-events:
			req.Respond(&msg.Term{Peers: term})
		}
	}
}

func (a *Actor) startActor(def *grid.ActorStart) error {
	peer, err := peerFromDef(def)
	if err != nil {
		return err
	}
	a.tracker.OptimisticallyRegister(def.Name, peer)
	_, err = a.send(30*time.Second, peer, def)
	if err != nil {
		a.tracker.OptimisticallyUnregister(def.Name)
		return err
	}
	return nil
}

func workerDef(peer string) *grid.ActorStart {
	def := grid.NewActorStart("worker-%v", peer)
	def.Type = "worker"
	def.Data = []byte(peer)
	return def
}

func peerFromDef(def *grid.ActorStart) (string, error) {
	if def.Type != "worker" {
		return "", fmt.Errorf("unkown worker type: %v", def.Type)
	}
	if string(def.Data) == "" {
		return "", fmt.Errorf("empty peer name in worker actor def")
	}
	return string(def.Data), nil
}
