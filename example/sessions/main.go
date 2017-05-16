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
	g.GroupBy(user)
	g.Window(window.Session(30 * time.Minute))
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
	User      string `json:"user"`
	URL       string `json:"url"`
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
				User:      v.User,
				URL:       v.URL,
			},
		}}, nil
	default:
		return nil, ErrUnknownType("clean", v)
	}
}

func user(v interface{}) (string, error) {
	switch v := v.(type) {
	case *Event:
		return v.User, nil
	default:
		return "", ErrUnknownType("user", v)
	}
}

func printer(span window.Span, vs []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("session: %v\n", span))
	for _, v := range vs {
		e, ok := v.(*Event)
		if !ok {
			return ErrUnknownType("printer", v)
		}
		buf.WriteString(fmt.Sprintf("    user: %4v, time: %v, url: %20v\n", e.User, e.Timestamp, e.URL))
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
