package leader

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

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

// Act as leader.
func (a *Actor) Act(ctx context.Context) {
	a.logger.Printf("running")

	a.eg, a.ctx = errgroup.WithContext(ctx)
	a.eg.Go(a.runPeerWatcher)
	a.eg.Go(a.runMailboxWatcher)
	err := a.eg.Wait()
	if err != nil {
		a.logger.Printf("failed with: %v", err)
	}

	a.logger.Printf("shutdown")
}

// String description of the watcher.
func (a *Actor) String() string {
	return "leader"
}

func (a *Actor) runPeerWatcher() error {
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
			case grid.EntityFound:
				a.tracker.Live(e.Peer())
				a.tracker.SetRequired(workerDef(e.Peer()))
			}
		}
	}
}

func (a *Actor) runMailboxWatcher() error {
	current, mailboxes, err := a.mailboxes(a.ctx)
	if err != nil {
		return err
	}
	for _, c := range current {
		a.tracker.Register(c.Name(), c.Peer())
	}

	missingTimer := time.NewTimer(1 * time.Second)
	defer missingTimer.Stop()

	relocateTimer := time.NewTimer(10 * time.Minute)
	defer relocateTimer.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case <-missingTimer.C:
			cnt := 0
			for _, def := range a.tracker.Missing() {
				cnt++
				err := a.startActor(def)
				if err != nil {
					a.logger.Printf("failed to start actor: %v, error: %v", def.Name, err)
				}
			}
			if cnt > 100 {
				missingTimer.Reset(20 * time.Second)
			} else {
				missingTimer.Reset(10 * time.Second)
			}
		case <-relocateTimer.C:
			a.logger.Printf("checking for relocations")
			plan := a.tracker.Relocate()
			a.logger.Printf("executing relocation plan: %v", plan)
			for _, def := range plan.Relocations {
				a.logger.Printf("relocating actor: %v", def.Name)
			}
			relocateTimer.Reset(10 * time.Minute)
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
