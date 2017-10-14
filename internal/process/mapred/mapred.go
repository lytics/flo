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
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/flo/sink"
	"github.com/lytics/flo/source"
	"github.com/lytics/grid"
	"golang.org/x/sync/errgroup"
)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

// New map and reduce process.
func New(parent, graphType, graphName string, conf []byte, def *graph.Definition, db *txdb.Bucket, s Send, l Listen) *Process {
	id := fmt.Sprintf("%v-%v-%v", parent, graphType, graphName)
	return &Process{
		id:        id,
		graphType: graphType,
		graphName: graphName,
		db:        db,
		def:       def,
		conf:      conf,
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
	db        *txdb.Bucket
	def       *graph.Definition
	conf      []byte
	logger    *log.Logger
	ring      *schedule.Ring
	schedule  chan *schedule.Ring
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

	r, open := <-p.schedule
	if !open {
		return fmt.Errorf("schedule closed before ever being defined")
	}
	p.ring = r

	var err error

	p.sources, err = p.def.From().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	p.sinks, err = p.def.Into().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	p.logger.Printf("reducer listening to mailbox: %v", p.id)
	messages, close, err := p.listen(p.id)
	if err != nil {
		return err
	}
	defer close()
	p.messages = messages

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	p.ctx = ctx
	p.cancel = cancel

	eg.Go(p.runMap)
	eg.Go(p.runRed)
	eg.Go(p.runTrig)

	return eg.Wait()
}

func (p *Process) SetRing(r *schedule.Ring) {
	select {
	case p.schedule <- r:
		p.logger.Printf("received ring: %v", r)
	default:
	}
}

func (p *Process) runMap() error {
	defer p.logger.Print("mapper exiting")
	p.logger.Print("mapper running")

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
		p.db.Finish(src.Metadata().Name)
	}

	return nil
}

func (p *Process) runRed() error {
	defer p.logger.Printf("reducer exiting")
	p.logger.Print("reducer running")

	for {
		select {
		case <-p.ctx.Done():
			return nil
		case req := <-p.messages:
			switch m := req.Msg().(type) {
			case *msg.Keyed:
				v, err := codec.Unmarshal(m.Data, m.DataType)
				if err != nil {
					req.Respond(err)
					continue
				}
				err = p.reduce(graph.KeyedEvent{
					Time: m.EventTime(),
					Key:  m.Key,
					Msg:  v,
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
	defer p.logger.Printf("trigger exiting")
	p.logger.Print("trigger running")

	signal := func(keys []string) {
		p.db.Drain(keys, p.sinks[0].Give)
	}
	return p.def.Trigger().Start(signal)
}

// Stop mapping, reducing and triggering.
func (p *Process) Stop() {
	p.def.Trigger().Stop()
	p.cancel()
	p.logger.Printf("stopping")
}
