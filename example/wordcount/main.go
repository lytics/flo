package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/source/linefile"
	"github.com/lytics/flo/source/md5id"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

func main() {
	g := graph.New("wordcount")
	g.From(linefile.FromFile("words.txt"))
	g.Transform(clean)
	g.GroupBy(word)
	g.Merger(adder)
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
	line := v.(string)
	line = strings.Trim(line, "\n.!@#$%^&*()")
	words := strings.Split(line, " ")

	ws := []graph.Event{}
	for _, w := range words {
		if w == "" {
			continue
		}
		ws = append(ws, graph.Event{
			TS: time.Now(),
			ID: md5id.FromString(w),
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

func printer(span window.Span, key string, vs []interface{}) error {
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

func init() {
	flo.Register(Word{})
}
