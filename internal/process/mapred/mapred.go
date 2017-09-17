package mapred

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/flo/sink"
	"github.com/lytics/flo/source"
	"github.com/lytics/grid"
)

type Send func(timeout time.Duration, receiver string, msg interface{}) (interface{}, error)

type Listen func(name string) (<-chan grid.Request, func() error, error)

// New mapper.
func New(id, graphType, graphName string, conf []byte, def *graph.Definition, db *txdb.Bucket, s Send, l Listen) *Process {
	return &Process{
		id:        id,
		graphType: graphType,
		graphName: graphName,
		db:        db,
		def:       def,
		conf:      conf,
		send:      s,
		listen:    l,
		logger:    log.New(os.Stderr, id+"-"+graphType+"-"+graphName+": ", log.LstdFlags),
	}
}

// Process for mapping source data.
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
	send      Send
	listen    Listen
	sources   []source.Source
	sinks     []sink.Sink
	messages  <-chan grid.Request
	receivers []string
}

// Run process.
func (p *Process) Run() error {
	p.logger.Printf("starting")

	for i := 0; i < 10; i++ {
		p.receivers = append(p.receivers, fmt.Sprintf("worker-%d-%v-%v", i, p.graphType, p.graphName))
	}

	var err error

	p.sources, err = p.def.From().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	p.sinks, err = p.def.Into().Setup(p.graphType, p.graphName, p.conf)
	if err != nil {
		return err
	}

	messages, close, err := p.listen(fmt.Sprintf("%v-%v-%v", p.id, p.graphType, p.graphName))
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

func (p *Process) runMap() error {
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
	signal := func(keys []string) {
		p.db.Drain(keys, p.sinks[0].Give)
	}
	return p.def.Trigger().Start(signal)
}

// Stop mapping, reducing and triggering.
func (p *Process) Stop() {
	p.def.Trigger().Stop()
	p.cancel()
}
