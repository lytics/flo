package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"bytes"

	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/source/jsonfile"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

func ErrUnknownType(msg string, v interface{}) error {
	return fmt.Errorf("main: %v: unknown type: %T", msg, v)
}

func main() {
	g := flo.New(flo.GraphCfg{Name: "events"})
	g.From(jsonfile.FromObjects(Entry{}, "event.data"))
	g.Transform(clean)
	g.Window(window.Sliding(1*time.Hour, 1*time.Hour))
	g.Trigger(trigger.AtPeriod(10 * time.Second))
	g.Into(printer)

	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	op, err := flo.NewOperator(etcd, flo.OperatorCfg{Namespace: "example"})
	successOrDie(err)

	lis, err := net.Listen("tcp", "localhost:0")
	successOrDie(err)

	go func() {
		err := op.Serve(lis)
		successOrDie(err)
	}()

	op.RunGraph(g)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	<-sig
	op.TerminateGraph(g)

	op.Stop()
}

type Entry struct {
	Timestamp string `json:"ts"`
	Data      int64  `json:"data"`
}

func clean(v interface{}) ([]graph.Event, error) {
	switch v := v.(type) {
	case *Entry:
		ts, err := time.Parse(time.RFC3339, v.Timestamp)
		if err != nil {
			return nil, err
		}
		return []graph.Event{{
			TS: ts,
			Msg: &Event{
				Timestamp: v.Timestamp,
				Data:      v.Data,
			},
		}}, nil
	default:
		return nil, ErrUnknownType("clean", v)
	}
}

func printer(span window.Span, vs []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("window: %v\n", span))
	for _, v := range vs {
		e, ok := v.(*Event)
		if !ok {
			return ErrUnknownType("printer", v)
		}
		buf.WriteString(fmt.Sprintf("    data: %4v, time: %v\n", e.Data, e.Timestamp))
	}
	fmt.Println(buf.String())
	return nil
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	flo.Register(Event{})
}
