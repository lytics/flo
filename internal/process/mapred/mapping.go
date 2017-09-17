package mapred

import (
	"hash/fnv"
	"io"
	"time"

	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/internal/codec"
	"github.com/lytics/flo/internal/msg"
	"github.com/lytics/flo/source"
)

func (p *Process) consume(src source.Source) error {
	err := src.Init()
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
		_, v, err := src.Take(p.ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = p.process(v)
		if err != nil {
			return err
		}
	}
}

func (p *Process) process(v interface{}) error {
	if v == nil {
		return nil
	}
	keyedEvents, err := p.transformAndGroupAndWindow(v)
	if err != nil {
		return err
	}
	for _, ke := range keyedEvents {
		err := p.shuffle(ke.Key, ke.Time, ke.Msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Process) transformAndGroupAndWindow(v interface{}) ([]graph.KeyedEvent, error) {
	events, err := p.def.Transform(v)
	if err != nil {
		return nil, err
	}
	var keyedEvents []graph.KeyedEvent
	for _, e := range events {
		grouped, err := p.def.GroupAndWindowBy(e.ID, e.Time, e.Msg)
		if err != nil {
			return nil, err
		}
		keyedEvents = append(keyedEvents, grouped...)
	}
	return keyedEvents, nil
}

func (p *Process) shuffle(key string, ts time.Time, v interface{}) error {
	dataType, data, err := codec.Marshal(v)
	if err != nil {
		return err
	}
	receiver := p.receiverByKey(key)
	_, err = p.send(10*time.Second, receiver, &msg.Keyed{
		TS:       ts.Unix(),
		Key:      key,
		Data:     data,
		DataType: dataType,
	})
	return err
}

func (p *Process) receiverByKey(key string) string {
	h := fnv.New64()
	h.Write([]byte(key))
	v := h.Sum64()
	return p.receivers[int(v%uint64(len(p.receivers)))]
}
