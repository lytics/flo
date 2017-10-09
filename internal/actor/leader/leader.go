package leader

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lytics/flo/internal/msg"

	"golang.org/x/sync/errgroup"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/peerqueue"
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

// New peer watcher.
func New(db *txdb.DB, d Define, s Send, l Listen, w Watch, p Peers, m Mailboxes) (*Actor, error) {
	return &Actor{
		logger:    log.New(os.Stderr, "leader: ", log.LstdFlags),
		tracker:   peerqueue.New(),
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
	eg      *errgroup.Group
	ctx     context.Context
	name    string
	logger  *log.Logger
	tracker *peerqueue.PeerQueue
	// Outside world
	db        *txdb.DB
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
		a.tracker.Live(c.Peer())
		a.tracker.SetRequired(workerDef(c.Peer()))
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
				a.tracker.Live(e.Peer())
				a.tracker.SetRequired(workerDef(e.Peer()))
			}
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

	missingTimer := time.NewTimer(500 * time.Millisecond)
	defer missingTimer.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case <-missingTimer.C:
			for _, def := range a.tracker.Missing() {
				err := a.startActor(def)
				if err != nil {
					a.logger.Printf("failed to start actor: %v, error: %v", def.Name, err)
				}
			}
		case e := <-mailboxes:
			a.logger.Printf("%v", e)
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
		<-time.After(5 * time.Second)
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
