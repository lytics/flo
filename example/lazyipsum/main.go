package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/sink"
	"github.com/lytics/flo/sink/funcsink"
	"github.com/lytics/flo/source"
	"github.com/lytics/flo/source/linefile"
	"github.com/lytics/flo/source/md5id"
	_ "github.com/lytics/flo/storage/driver/memdriver"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

// WithoutConf is a nil configuration for graphs.
var WithoutConf = []byte(nil)

func main() {
	// Build graph definition.
	g := graph.New()
	g.From(source.SkipSetup(linefile.FromFile("words.txt")))
	g.Transform(clean)
	g.Group(word)
	g.Merger(adder)
	g.Trigger(trigger.WhenDormant(2 * time.Second))
	g.Into(sink.SkipSetup(funcsink.New(print)))

	// Register our message type, and graph type.
	flo.RegisterMsg(Word{})
	flo.RegisterGraph("lazyipsum", g)

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

	// Run a default instance of the lazyipsum graph.
	// Multiple instances of the same graph type
	// can be run, but in this example only one
	// is run.
	err = client.RunGraph("lazyipsum", "default", WithoutConf)
	successOrDie(err)

	// Wait for a user interrupt.
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	<-sig

	// Terminate the default instance of the lazyipsum graph.
	err = client.TerminateGraph("lazyipsum", "default")
	successOrDie(err)

	fmt.Println("stopped, bye bye")
}

func clean(v interface{}) ([]graph.Event, error) {
	line := v.(string)
	line = strings.Trim(line, "\n.!@#$%^&*()")
	words := strings.Split(line, " ")

	ws := []graph.Event{}
	for _, w := range words {
		if w == "" {
			continue
		}
		ws = append(ws, graph.Event{
			Time: time.Now(),
			ID:   md5id.FromString(w),
			Msg: &Word{
				Text:  w,
				Count: 1,
			},
		})
	}
	return ws, nil
}

func word(w interface{}) (string, error) {
	return w.(*Word).Text, nil
}

func adder(a, b interface{}) (interface{}, error) {
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}
	aw := a.(*Word)
	bw := b.(*Word)

	aw.Combine(bw)
	return aw, nil
}

func print(ctx context.Context, span window.Span, key string, vs []interface{}) error {
	for _, v := range vs {
		w := v.(*Word)
		fmt.Printf("word: %20v, count: %10d, time window: %v\n", w.Text, w.Count, span)
	}
	return nil
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
