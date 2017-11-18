package main

import (
	"bytes"
	"context"
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

// Entry exists just to map JSON
// data to a struct.
type Entry struct {
	Timestamp string `json:"ts"`
	User      string `json:"user"`
	URL       string `json:"url"`
}

func main() {
	// Define the graph of processig elements.
	g := graph.New()
	g.From(source.SkipSetup(jsonfile.New(Entry{}, "datafile/file1.data")))
	g.Transform(clean)
	g.Group(user)
	g.Window(window.Fixed(1 * time.Hour))
	g.Trigger(trigger.WhenFinished())
	g.Into(sink.SkipSetup(funcsink.New(print)))

	// Register our message type, and graph type.
	flo.RegisterMsg(Event{})
	flo.RegisterGraph("batch", g)

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

	// Run a default instance of the batch graph.
	// Multiple instances of the same graph type
	// can be run, but in this example only one
	// is run.
	err = client.RunGraph("batch", "default", WithoutConf)
	successOrDie(err)

	// Wait for a user interrupt.
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	<-sig

	// Terminate the default instance of the batch graph.
	err = client.TerminateGraph("batch", "default")
	successOrDie(err)

	fmt.Println("killed, bye bye")
}

func clean(v interface{}) ([]graph.Event, error) {
	e := v.(*Entry)
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

func print(ctx context.Context, span window.Span, key string, vs []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("window: %v\n", span))
	for _, v := range vs {
		e := v.(*Event)
		buf.WriteString(fmt.Sprintf("    url: %4v, time: %v\n", e.URL, e.Timestamp))
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
