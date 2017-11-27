package mapred

import (
	"io"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/source"
	"github.com/lytics/retry"
)

func (p *Process) consume(src source.Source) error {
	err := src.Init(p.ctx, nil)
	if err != nil {
		return err
	}
	defer src.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return nil
		default:
		}
		item, err := src.Take(p.ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		retry.X(3, 10*time.Second, func() bool {
			err = p.process(item)
			return err != nil
		})
		if err != nil {
			return err
		}
	}
}

func (p *Process) process(item *source.Item) error {
	if item == nil || item.Value() == nil {
		return nil
	}
	events, err := p.def.Transform(item.Value())
	if err != nil {
		return err
	}
	events, err = p.groupAndWindow(events)
	if err != nil {
		return err
	}
	for _, e := range events {
		err := p.shuffle(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Process) groupAndWindow(events []graph.Event) ([]graph.Event, error) {
	var grouped []graph.Event
	for _, e := range events {
		es, err := p.def.GroupAndWindowBy(e)
		if err != nil {
			return nil, err
		}
		grouped = append(grouped, es...)
	}
	return grouped, nil
}

func (p *Process) shuffle(e graph.Event) error {
	dataType, data, err := codec.Marshal(e.Data)
	if err != nil {
		return err
	}
	receiver := p.ring.Reducer(e.Key, p.graphType, p.graphName)
	p.logger.Printf("sending to: %v, event: (%v)", receiver, e.Data)
	_, err = p.send(10*time.Second, receiver, &msg.Event{
		Key:       e.Key,
		Data:      data,
		DataType:  dataType,
		SpanEnd:   e.Span.End(),
		SpanStart: e.Span.Start(),
	})
	return err
}
