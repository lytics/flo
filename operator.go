package flo

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo/actor/leader"
	"github.com/lytics/flo/actor/worker"
	"github.com/lytics/flo/codec"
	"github.com/lytics/flo/graph"
	"github.com/lytics/flo/msg"
	"github.com/lytics/grid/grid.v3"
)

var ErrInvalidNamespace = errors.New("invalid namespace")

// Register a value for use, where value must
// be a protobuf message type.
func Register(v interface{}) error {
	return codec.Register(v)
}

type OperatorCfg struct {
	Namespace string
}

func NewOperator(etcd *etcdv3.Client, cfg OperatorCfg) (*Operator, error) {
	if cfg.Namespace == "" {
		return nil, ErrInvalidNamespace
	}

	logger := log.New(os.Stdout, "operator: ", log.LstdFlags)

	op := &Operator{
		cfg:    &cfg,
		etcd:   etcd,
		ready:  make(chan bool),
		graphs: graphs{},
		logger: logger,
	}

	server, err := grid.NewServer(etcd, grid.ServerCfg{
		Namespace: cfg.Namespace,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	op.server = server

	client, err := grid.NewClient(etcd, grid.ClientCfg{
		Namespace: cfg.Namespace,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	op.client = client

	server.RegisterDef("leader", func([]byte) (grid.Actor, error) {
		return leader.New(client, server)
	})
	server.RegisterDef("worker", func([]byte) (grid.Actor, error) {
		return worker.New(client, server, op.graphs)
	})

	go op.runWorkerWatcher()
	return op, nil
}

type Operator struct {
	mu        sync.Mutex
	cfg       *OperatorCfg
	etcd      *etcdv3.Client
	client    *grid.Client
	server    *grid.Server
	receivers []string
	ready     chan bool
	logger    *log.Logger
	graphs    graphs
}

func (op *Operator) Serve(lis net.Listener) error {
	return op.server.Serve(lis)
}

func (op *Operator) Stop() {
	op.server.Stop()
	op.client.Close()
}

func (op *Operator) RunGraph(g *graph.Graph) {
	op.mu.Lock()
	defer op.mu.Unlock()

	def := g.Definition()

	co, ok := op.graphs[def.Name()]
	if ok {
		return
	}
	co = newCoordinator(def, op)
	op.graphs[def.Name()] = co

	<-op.ready
	go co.run()
}

func (op *Operator) TerminateGraph(g *graph.Graph) {
	op.mu.Lock()
	defer op.mu.Unlock()

	co, ok := op.graphs[g.Name()]
	if !ok {
		return
	}
	co.feederCancel()
}

func (op *Operator) runWorkerWatcher() {
	<-time.After(1 * time.Second)

	current, mailboxes, err := op.client.QueryWatch(op.server.Context(), grid.Mailboxes)
	if err != nil {
		panic(err)
	}
	defer close(op.ready)

	for _, c := range current {
		if strings.HasPrefix(c.Name(), "worker") {
			op.receivers = append(op.receivers, c.Name())
		}
	}
	if len(op.receivers) == 10 {
		return
	}

	for {
		e := <-mailboxes
		if e.Type == grid.EntityFound {
			if strings.HasPrefix(e.Name(), "worker") {
				op.receivers = append(op.receivers, e.Name())
			}
		}
		if len(op.receivers) == 10 {
			return
		}
	}
}

func (op *Operator) receiverByKey(key string) string {
	h := fnv.New64()
	h.Write([]byte(key))
	v := h.Sum64()
	return op.receivers[int(v%uint64(len(op.receivers)))]
}

func (op *Operator) sendByKey(graph, key string, ts time.Time, v interface{}) error {
	dataType, data, err := codec.Marshal(v)
	if err != nil {
		return err
	}
	receiver := op.receiverByKey(key)
	_, err = op.client.Request(10*time.Second, receiver, &msg.Keyed{
		Key:      key,
		Unix:     ts.Unix(),
		Data:     data,
		DataType: dataType,
		Graph:    graph,
	})
	if err != nil {
		return fmt.Errorf("key: %v, receiver: %v, error: %v", key, receiver, err)
	}
	return nil
}

type graphs map[string]*coordinator

func (gs graphs) Lookup(name string) graph.Reducer {
	g, ok := gs[name]
	if !ok {
		return nil
	}
	return g
}
