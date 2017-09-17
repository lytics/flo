package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/sink"
	"github.com/lytics/flo/sink/printer"
	"github.com/lytics/flo/source"
	"github.com/lytics/flo/source/jsonfile"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

// WithoutConf is a nil configuration for graphs.
var WithoutConf = []byte(nil)

func main() {
	// Build graph definition.
	g := graph.New()
	g.From(source.WrapSources(jsonfile.FromFile(Event{}, "event.data")))
	g.Transform(clean)
	g.Group(user)
	g.Window(window.Session(30 * time.Minute))
	g.Trigger(trigger.AtPeriod(10 * time.Second))
	g.Into(sink.WrapSinks(printer.FromFunc(print)))

	// Register our message type, and graph type.
	flo.RegisterMsg(Event{})
	flo.RegisterGraph("sessions", g)

	// Create etcd v3 client.
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	// Create the flo config.
	cfg := flo.Cfg{Namespace: "example"}

	// Create the flo client.
	client, err := flo.NewClient(etcd, cfg)
	successOrDie(err)

	// Create the flo server.
	server, err := flo.NewServer(etcd, cfg)
	successOrDie(err)

	// Create a listener.
	lis, err := net.Listen("tcp", "localhost:0")
	successOrDie(err)

	// Have the server serve our graphs.
	go func() {
		err := server.Serve(lis)
		successOrDie(err)
	}()
	defer server.Stop()

	// Run a default instance of the sessions graph.
	// Multiple instances of the same graph type
	// can be run, but in this example only one
	// is run.
	err = client.RunGraph("sessions", "default", WithoutConf)
	successOrDie(err)

	// Wait for a user interrupt.
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	<-sig

	// Terminate the default instance of the sessions graph.
	err = client.TerminateGraph("sessions", "default")
	successOrDie(err)
}

func clean(v interface{}) ([]graph.Event, error) {
	e := v.(*Event)
	ts, err := time.Parse(time.RFC3339, e.Timestamp)
	if err != nil {
		return nil, err
	}
	return []graph.Event{{
		Time: ts,
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

func print(span window.Span, key string, vs []interface{}) error {
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
