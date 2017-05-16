package main

import (
	"crypto/md5"
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
	"github.com/lytics/flo/source/slowdown"
	"github.com/lytics/flo/trigger"
	"github.com/lytics/flo/window"
)

func ErrUnknownType(msg string, v interface{}) error {
	return fmt.Errorf("main: %v: unknown type: %T", msg, v)
}

func main() {
	g := graph.New("wordcount")
	g.From(slowdown.Wrap(0.5, linefile.New("words.txt")))
	g.Transform(clean)
	g.GroupBy(word)
	g.Merger(adder)
	g.Trigger(trigger.WhenDormant(2 * time.Second))
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

func clean(w interface{}) ([]graph.Event, error) {
	switch w := w.(type) {
	case string:
		w = strings.Trim(w, "\n.!@#$%^&*()")
		ss := strings.Split(w, " ")
		ws := []graph.Event{}
		for _, s := range ss {
			if s == "" {
				continue
			}
			h := md5.New()
			h.Write([]byte(s))
			ws = append(ws, graph.Event{
				TS: time.Now(),
				ID: string(h.Sum(nil)),
				Msg: &Word{
					Text:  s,
					Count: 1,
				},
			})
		}
		return ws, nil
	default:
		return nil, ErrUnknownType("clean", w)
	}
}

func word(w interface{}) (string, error) {
	switch w := w.(type) {
	case *Word:
		return w.Text, nil
	default:
		return "", ErrUnknownType("word", w)
	}
}

func adder(a, b interface{}) (interface{}, error) {
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}
	aw, ok := a.(*Word)
	if !ok {
		return nil, ErrUnknownType("adder", a)
	}
	bw, ok := b.(*Word)
	if !ok {
		return nil, ErrUnknownType("adder", b)
	}

	aw.Combine(bw)
	return aw, nil
}

func printer(span window.Span, key string, vs []interface{}) error {
	for _, v := range vs {
		w, ok := v.(*Word)
		if !ok {
			return ErrUnknownType("printer", v)
		}
		if key != w.Text {
			panic("text and key do not match")
		}
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
