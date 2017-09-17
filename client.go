package flo

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/flo/internal/registry"
)

// NewClient to start and stop graphs.
func NewClient(etcd *etcdv3.Client, cfg Cfg) (*Client, error) {
	if cfg.Namespace == "" {
		return nil, ErrInvalidNamespace
	}

	reg, err := registry.New(etcd, cfg.Namespace)
	if err != nil {
		return nil, err
	}

	op := &Client{
		cfg:      &cfg,
		etcd:     etcd,
		logger:   log.New(os.Stdout, cfg.Namespace+": ", log.LstdFlags),
		registry: reg,
	}

	return op, nil
}

// Client for graphs.
type Client struct {
	mu       sync.Mutex
	cfg      *Cfg
	etcd     *etcdv3.Client
	logger   *log.Logger
	registry *registry.Registry
}

// RunGraph of the given type and name.
func (c *Client) RunGraph(graphType, graphName string, config []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.registry.Insert(timeout, graphType, graphName, registry.Running, config)
	if err == registry.ErrAlreadyInserted {
		return c.registry.SetWanted(timeout, graphType, graphName, registry.Running)
	}
	return err
}

// TerminateGraph of the given type and name.
func (c *Client) TerminateGraph(graphType, graphName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.registry.SetWanted(timeout, graphType, graphName, registry.Terminating)
}

// Watch for graph registration and other lifecycle events.
func (c *Client) Watch(ctx context.Context) ([]*registry.WatchEvent, <-chan *registry.WatchEvent, error) {
	return c.registry.Watch(ctx)
}
