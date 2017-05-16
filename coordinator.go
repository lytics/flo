package flo

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/process"
	"github.com/lytics/flo/process/feeder"
	"github.com/lytics/flo/process/mapper"
	"github.com/lytics/flo/txdb"
)

func newCoordinator(def *graph.Definition, op *Operator) *coordinator {
	return &coordinator{
		op:     op,
		db:     txdb.New(def.Name()),
		def:    def,
		logger: log.New(os.Stderr, def.Name()+": ", log.LstdFlags),
	}
}

type coordinator struct {
	op     *Operator
	db     *txdb.DB
	def    *graph.Definition
	logger *log.Logger

	feederCtx    context.Context
	feederCancel func()
	mapperCtx    context.Context
	mapperCancel func()
}

func (co *coordinator) Reduce(m graph.KeyedEvent) error {
	err := co.db.Apply(m.Key, func(row *txdb.Row) error {
		err := co.def.Merge(&m, row.Windows)
		if err != nil {
			return err
		}
		return co.def.Trigger().Modified(m.Key, m.Msg, row.Snapshot().Windows)
	})
	if err != nil {
		return err
	}

	return nil
}

func (co *coordinator) run() {
	co.feederCtx, co.feederCancel = context.WithCancel(context.Background())
	co.mapperCtx, co.mapperCancel = context.WithCancel(context.Background())

	work := make(chan *process.Task)
	fail := make(chan error, 1)

	putError := func(detail string, err error) {
		if err == nil {
			return
		}
		select {
		case fail <- err:
			fmt.Fprintf(os.Stderr, "error: %v: %v\n", detail, err)
		default:
		}
	}

	putTask := func(ctx context.Context, task *process.Task) {
		select {
		case <-ctx.Done():
			return
		case work <- task:
		}
	}

	putMsg := func(key string, ts time.Time, v interface{}) error {
		return co.op.sendByKey(co.def.Name(), key, ts, v)
	}

	go co.def.Trigger().Start(func(keys []string) {
		co.db.Drain(keys, co.def.Into())
	})

	wgFeeders := &sync.WaitGroup{}
	for _, vs := range co.def.From() {
		wgFeeders.Add(1)
		f := feeder.New(putError, putTask, vs)
		go f.Run(co.feederCtx, wgFeeders)
	}
	wgMappers := &sync.WaitGroup{}
	for i := 0; i < 2*runtime.NumCPU(); i++ {
		wgMappers.Add(1)
		m := mapper.New(putError, putMsg, co.def, work)
		go m.Run(co.mapperCtx, wgMappers)
	}

	wgFeeders.Wait()
	wgMappers.Wait()
}
