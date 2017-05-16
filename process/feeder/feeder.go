package feeder

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/process"
)

var (
	ErrNilValue = errors.New("nil value")
)

type PutTask func(context.Context, *process.Task)

type PutError func(string, error)

type Process struct {
	vs       graph.Collection
	putTask  PutTask
	putError PutError
}

func New(e PutError, t PutTask, vs graph.Collection) *Process {
	return &Process{
		vs:       vs,
		putTask:  t,
		putError: e,
	}
}

func (p *Process) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	err := p.vs.Init()
	if err != nil {
		p.putError("init", err)
		return
	}
	defer func() {
		err := p.vs.Close()
		if err != nil {
			p.putError("close", err)
		}
	}()
	for {
		id, msg, err := p.vs.Next(ctx)
		if err == io.EOF {
			return
		}
		if err != nil {
			p.putError("next value", err)
			return
		}
		if msg == nil {
			p.putError("next value", ErrNilValue)
			return
		}
		p.putTask(ctx, &process.Task{
			ID:     id,
			Msg:    msg,
			Source: p.vs.Name(),
		})
	}
}
