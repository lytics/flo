package flo

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo/internal/actor/leader"
	"github.com/lytics/flo/internal/actor/worker"
	"github.com/lytics/flo/internal/registry"
	"github.com/lytics/flo/internal/txdb"
	"github.com/lytics/grid"
)

func NewServer(etcd *etcdv3.Client, cfg Cfg) (*Server, error) {
	if cfg.Namespace == "" {
		return nil, ErrInvalidNamespace
	}

	logger := log.New(os.Stdout, cfg.Namespace+": ", log.LstdFlags)

	s := &Server{
		cfg:    &cfg,
		etcd:   etcd,
		logger: logger,
	}

	server, err := grid.NewServer(etcd, grid.ServerCfg{
		Namespace: cfg.Namespace,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	s.server = server

	client, err := grid.NewClient(etcd, grid.ClientCfg{
		Namespace: cfg.Namespace,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	s.client = client

	reg, err := registry.New(etcd, cfg.Namespace)
	if err != nil {
		return nil, err
	}
	s.registry = reg

	open := func(name string) (*txdb.DB, error) {
		return txdb.Open(cfg.Driver, name)
	}

	send := client.Request

	listen := func(name string) (<-chan grid.Request, func() error, error) {
		mailbox, err := grid.NewMailbox(server, name, 100)
		if err != nil {
			return nil, nil, err
		}
		return mailbox.C, mailbox.Close, nil
	}

	watch := func(ctx context.Context) ([]*registry.WatchEvent, <-chan *registry.WatchEvent, error) {
		return reg.Watch(ctx)
	}

	peers := func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error) {
		return client.QueryWatch(ctx, grid.Peers)
	}

	mailboxes := func(ctx context.Context) ([]*grid.QueryEvent, <-chan *grid.QueryEvent, error) {
		return client.QueryWatch(ctx, grid.Actors)
	}

	server.RegisterDef("leader", func([]byte) (grid.Actor, error) {
		return leader.New(
			LookupGraph,
			leader.Send(send),
			leader.Listen(listen),
			leader.Watch(watch),
			leader.Peers(peers),
			leader.Mailboxes(mailboxes))
	})

	server.RegisterDef("worker", func([]byte) (grid.Actor, error) {
		return worker.New(
			LookupGraph,
			worker.Open(open),
			worker.Send(send),
			worker.Listen(listen),
			worker.Watch(watch),
			worker.Peers(peers),
			worker.Mailboxes(mailboxes))
	})

	return s, nil
}

type Server struct {
	mu        sync.Mutex
	cfg       *Cfg
	etcd      *etcdv3.Client
	client    *grid.Client
	server    *grid.Server
	receivers []string
	logger    *log.Logger
	registry  *registry.Registry
}

func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

func (s *Server) Stop() {
	s.server.Stop()
}
