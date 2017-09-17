package leader

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

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
	tracker := peerqueue.New()
	for i := 0; i < 10; i++ {
		def := grid.NewActorStart("worker-%d", i)
		def.Type = "worker"
		tracker.SetRequired(def)
	}

	return &Actor{
		logger:    log.New(os.Stderr, "leader: ", log.LstdFlags),
		tracker:   tracker,
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
	ctx     context.Context
	cancel  func()
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
	a.logger.Printf("leader is running")
	// Create another context that will control
	// the exit of the helper go-routines. They
	// need to continue to run after an exit
	// has been request, for the sake of an
	// ordered "shutdown".
	a.ctx, a.cancel = context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go a.runPeerWatcher(wg)
	go a.runMailboxWatcher(wg)

	<-ctx.Done()
	a.cancel()

	wg.Wait()
	a.logger.Printf("shutdown")
}

// String description of the watcher.
func (a *Actor) String() string {
	return "leader"
}

func (a *Actor) runPeerWatcher(wg *sync.WaitGroup) {
	defer wg.Done()

	current, peers, err := a.peers(a.ctx)
	if err != nil {
		a.logger.Printf("failed creating peer watch: %v", err)
		a.cancel()
	}
	for _, c := range current {
		a.tracker.Live(c.Peer())
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		case e := <-peers:
			switch e.Type {
			case grid.WatchError:
				a.cancel()
			case grid.EntityLost:
				a.tracker.Dead(e.Peer())
			case grid.EntityFound:
				a.tracker.Live(e.Peer())
			}
		}
	}
}

func (a *Actor) runMailboxWatcher(wg *sync.WaitGroup) {
	defer wg.Done()

	current, mailboxes, err := a.mailboxes(a.ctx)
	if err != nil {
		a.logger.Printf("failed creating mailbox watch: %v", err)
		a.cancel()
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
			return
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
				a.logger.Printf("fatal error: %v", e.Err())
				a.cancel()
			case grid.EntityLost:
				a.tracker.Unregister(e.Name())
			case grid.EntityFound:
				a.tracker.Register(e.Name(), e.Peer())
			}
		}
	}
}

func (a *Actor) startActor(def *grid.ActorStart) error {
	peer, err := a.tracker.MinAssigned()
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
