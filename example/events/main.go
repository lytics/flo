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
	"github.com/lytics/flo/sink/funcsink"
	"github.com/lytics/flo/source"
	"github.com/lytics/flo/source/jsonfile"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

// WithoutConf is a nil configuration for graphs.
var WithoutConf = []byte(nil)

// Entry exists just to map data inside
// JSON files to a struct.
type Entry struct {
	Timestamp string `json:"ts"`
	Data      int64  `json:"data"`
}

func main() {
	// Define the graph.
	g := graph.New()
	g.From(source.SkipSetup(jsonfile.New(Entry{}, "event.data")))
	g.Transform(clean)
	g.Window(window.Sliding(1*time.Hour, 1*time.Hour))
	g.Trigger(trigger.AtPeriod(10 * time.Second))
	g.Into(sink.SkipSetup(funcsink.New(printer)))

	// Register our message type, and graph type.
	flo.RegisterMsg(Event{})
	flo.RegisterGraph("events", g)

	// Create etcd v3 client.
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	// Create the flo config, the only required
	// field is the namespace.
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

	// Run a default instance of the events graph.
	// Multiple instances of the same graph type
	// can be run, but in this example only one
	// is run.
	err = client.RunGraph("events", "default", WithoutConf)
	successOrDie(err)

	// Wait for a user interrupt.
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	<-sig

	// Terminate the default instance of the events graph.
	err = client.TerminateGraph("events", "default")
	successOrDie(err)

	fmt.Println("stopped, bye bye")
}

func clean(v interface{}) ([]graph.Event, error) {
	e := v.(*Entry)
	ts, err := time.Parse(time.RFC3339, e.Timestamp)
	if err != nil {
		return nil, err
	}
	msg := &Event{
		Timestamp: e.Timestamp,
		Data:      e.Data,
	}
	return []graph.Event{{
		Msg:  msg,
		Time: ts,
	}}, nil
}

func printer(span window.Span, key string, vs []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("window: %v\n", span))
	for _, v := range vs {
		e := v.(*Event)
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
