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

type Entry struct {
	Timestamp string `json:"ts"`
	User      string `json:"user"`
	URL       string `json:"url"`
}

func main() {
	g := graph.New("events")
	g.From(jsonfile.FromFile(Entry{}, "event.data"))
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

func clean(v interface{}) ([]graph.Event, error) {
	e := v.(*Entry)
	ts, err := time.Parse(time.RFC3339, e.Timestamp)
	if err != nil {
		return nil, err
	}
	return []graph.Event{{
		TS: ts,
		Msg: &Event{
			Timestamp: e.Timestamp,
			User:      e.User,
			URL:       e.URL,
		},
	}}, nil
}

func user(v interface{}) (string, error) {
	return v.(*Event).User, nil
}

func printer(span window.Span, key string, vs []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("session: %v\n", span))
	for _, v := range vs {
		e := v.(*Event)
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
