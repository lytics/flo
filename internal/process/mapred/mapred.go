package mapred

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/internal/schedule"
	"github.com/lytics/flo/sink"
	"github.com/lytics/flo/source"
	"github.com/lytics/flo/storage"
	"github.com/lytics/grid"
	"golang.org/x/sync/errgroup"
)

type Open func(name string) (*storage.DB, error)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

// New map and reduce process.
func New(parent, graphType, graphName string, conf []byte, def *graph.Definition, o Open, s Send, l Listen) *Process {
	id := fmt.Sprintf("%v-%v-%v", parent, graphType, graphName)
	return &Process{
		id:        id,
		graphType: graphType,
		graphName: graphName,
		def:       def,
		conf:      conf,
		open:      o,
		send:      s,
		listen:    l,
		schedule:  make(chan *schedule.Ring),
		logger:    log.New(os.Stderr, id+": ", log.LstdFlags),
	}
}

// Process for mapping and reducing.
type Process struct {
	id        string
	graphType string
	graphName string
	ctx       context.Context
	cancel    func()
	db        *storage.DB
	def       *graph.Definition
	conf      []byte
	logger    *log.Logger
	ring      *schedule.Ring
	schedule  chan *schedule.Ring
	open      Open
	send      Send
	listen    Listen
	sources   []source.Source
	sinks     []sink.Sink
	messages  <-chan grid.Request
	receivers []string
}

// String description of process.
func (p *Process) String() string {
	return p.id
}

// Run process.
func (p *Process) Run() error {
	p.logger.Printf("starting")
	defer p.logger.Printf("exited")

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	p.ctx = ctx
	p.cancel = cancel

	var err error

	p.db, err = p.open(p.id)
	if err != nil {
		return err
	}

	p.logger.Printf("waiting for ring")
	r, open := <-p.schedule
	if !open {
		return fmt.Errorf("schedule closed before ever being defined")
	}
	p.ring = r
	p.logger.Printf("received ring: %v", p.ring)

	p.sources, err = p.def.From().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	p.sinks, err = p.def.Into().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	messages, close, err := p.listen(p.id)
	if err != nil {
		return err
	}
	defer close()
	p.messages = messages

	eg.Go(p.runMap)
	eg.Go(p.runRed)
	eg.Go(p.runTrig)

	return eg.Wait()
}

func (p *Process) SetRing(r *schedule.Ring) {
	select {
	case p.schedule <- r:
	default:
	}
}

func (p *Process) runMap() error {
	p.logger.Print("mapper running")
	defer p.logger.Print("mapper exited")

	p.logger.Printf("mapper consuming %v sources", len(p.sources))
	for _, src := range p.sources {
		select {
		case <-p.ctx.Done():
			return nil
		default:
		}
		err := p.consume(src)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Process) runRed() error {
	p.logger.Print("reducer running")
	defer p.logger.Printf("reducer exited")

	for {
		select {
		case <-p.ctx.Done():
			return nil
		case req := <-p.messages:
			switch m := req.Msg().(type) {
			case *msg.Event:
				v, err := codec.Unmarshal(m.Data, m.DataType)
				if err != nil {
					req.Respond(err)
					continue
				}
				err = p.reduce(graph.Event{
					Key:    m.Key,
					Data:   v,
					Time:   m.Time(),
					Window: m.Window(),
				})
				if err != nil {
					req.Respond(err)
				} else {
					req.Ack()
				}
			}
		}
	}
}

func (p *Process) runTrig() error {
	p.logger.Print("trigger running")
	defer p.logger.Printf("trigger exited")

	signal := func(keys []string) error {
		for _, sink := range p.sinks {
			err := p.db.Drain(p.ctx, keys, sink.Give)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return p.def.Trigger().Start(signal)
}

// Stop mapping, reducing and triggering.
func (p *Process) Stop() {
	p.def.Trigger().Stop()
	p.cancel()
	p.logger.Printf("stopping")
}
