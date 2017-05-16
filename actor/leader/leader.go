package leader

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lytics/flo/peerqueue"
	"github.com/lytics/grid/grid.v3"
)

type Actor struct {
	ctx       context.Context
	cancel    func()
	client    *grid.Client
	server    *grid.Server
	logger    *log.Logger
	tracker   *peerqueue.PeerQueue
	helpersWg *sync.WaitGroup
	Timeout   time.Duration
}

// New peer watcher.
func New(c *grid.Client, s *grid.Server) (*Actor, error) {
	return &Actor{
		logger:  log.New(os.Stderr, "leader: ", log.LstdFlags),
		client:  c,
		server:  s,
		tracker: peerqueue.New(),
		Timeout: 10 * time.Second,
	}, nil
}

// Act as leader.
func (a *Actor) Act(c context.Context) {
	// Create another context that will control
	// the exit of the helper go-routines. They
	// need to continue to run after an exit
	// has been request, for the sake of an
	// ordered "shutdown".
	a.ctx, a.cancel = context.WithCancel(context.Background())
	defer a.cancel()

	a.helpersWg = &sync.WaitGroup{}
	a.helpersWg.Add(2)

	go a.runPeerWatcher()
	go a.runMailboxWatcher()

	a.helpersWg.Wait()
	a.log("shutdown")
}

// String description of the watcher.
func (a *Actor) String() string {
	return "leader"
}

func (a *Actor) runPeerWatcher() {
	defer a.helpersWg.Done()

	current, peers, err := a.client.QueryWatch(a.ctx, grid.Peers)
	if err != nil {
		a.log("failed creating peer watch: %v", err)
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

func (a *Actor) runMailboxWatcher() {
	defer a.helpersWg.Done()

	for i := 0; i < 10; i++ {
		def := grid.NewActorStart("worker-%d", i)
		def.Type = "worker"
		a.tracker.SetRequired(def)
	}

	current, mailboxes, err := a.client.QueryWatch(a.ctx, grid.Mailboxes)
	if err != nil {
		a.log("failed creating mailbox watch: %v", err)
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
					a.log("failed to start actor: %v, error: %v", def.Name, err)
				}
			}
			if cnt > 100 {
				missingTimer.Reset(20 * time.Second)
			} else {
				missingTimer.Reset(10 * time.Second)
			}
		case <-relocateTimer.C:
			defs := a.tracker.Relocate()
			for _, def := range defs {
				a.log("relocating actor: %v", def.Name)
				_, err := a.client.Request(a.Timeout, def.Name, nil)
				if err != nil {
					a.log("failed to relocate actor: %v, error: %v", def.Name, err)
				}
			}
			relocateTimer.Reset(10 * time.Minute)
		case e := <-mailboxes:
			a.log("%v", e)
			switch e.Type {
			case grid.WatchError:
				a.log("fatal error: %v", e.Err())
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
	peer, err := a.tracker.RoundRobin()
	if err != nil {
		return err
	}
	a.log("starting actor: %v, on peer: %v", def.Name, peer)
	_, err = a.client.Request(a.Timeout, peer, def)
	return err
}

func (a *Actor) log(format string, v ...interface{}) {
	a.logger.Printf(format, v...)
}
