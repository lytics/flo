package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	// ErrNilEtcd when etcd client is nil.
	ErrNilEtcd = errors.New("nil etcd")
	// ErrFailedInsert when etcd returns no error for a key
	// insert but the success flag is false.
	ErrFailedInsert = errors.New("failed insert")
	// ErrFailedDelete when etcd returns no error for a key
	// delete but the success flag is false.
	ErrFailedDelete = errors.New("failed delete")
	// ErrAlreadyDeleted when the given entry already exists.
	ErrAlreadyDeleted = errors.New("already deleted")
	// ErrAlreadyInserted when the given entry already exists.
	ErrAlreadyInserted = errors.New("already inserted")
	// ErrZeroValues when no value is found.
	ErrZeroValues = errors.New("zero values")
	// ErrMultipleValues when multiple values are found under one
	// key, meaning that the key is not a key but a prefix.
	ErrMultipleValues = errors.New("multiple values")
	// ErrWatchClosedUnexpectedly when a watch closes and no error
	// is reported by the etcd client.
	ErrWatchClosedUnexpectedly = errors.New("watch closed unexpectedly")
)

// Registration information.
type Registration struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Wanted string `json:"wanted"`
	Config string `json:"config"`
}

// UnmarshalConfig which has a base type of []byte
// but is marshalled as a base64 string.
func (r *Registration) UnmarshalConfig() ([]byte, error) {
	return base64.StdEncoding.DecodeString(r.Config)
}

// String descritpion of registration.
func (r *Registration) String() string {
	return fmt.Sprintf("name: %v, type: %v, wanted: %v, config size: %v",
		r.Type, r.Name, r.Wanted, len(r.Config))
}

// EventType of a watch event.
type EventType int

const (
	// Error in the watch.
	Error EventType = 0
	// Delete of a key and its value.
	Delete EventType = 1
	// Modify of a key's value.
	Modify EventType = 2
	// Create of a key and its value.
	Create EventType = 3
)

// WatchEvent triggred by a change in the registry.
type WatchEvent struct {
	Key   string
	Reg   *Registration
	Type  EventType
	Error error
}

// String representation of the watch event.
func (we *WatchEvent) String() string {
	if we.Error != nil {
		return fmt.Sprintf("key: %v, error: %v", we.Key, we.Error)
	}
	typ := "delete"
	switch we.Type {
	case Modify:
		typ = "modify"
	case Create:
		typ = "create"
	}
	return fmt.Sprintf("key: %v, type: %v, registration: %v", we.Key, typ, we.Reg)
}

// Registry for discovery.
type Registry struct {
	mu        sync.Mutex
	kv        etcdv3.KV
	client    *etcdv3.Client
	namespace string
	Logger    *log.Logger
}

// New Registry.
func New(client *etcdv3.Client, namespace string) (*Registry, error) {
	if client == nil {
		return nil, ErrNilEtcd
	}
	return &Registry{
		kv:        etcdv3.NewKV(client),
		client:    client,
		namespace: namespace,
	}, nil
}

// Watch a prefix in the registry.
func (rr *Registry) Watch(ctx context.Context) ([]*WatchEvent, <-chan *WatchEvent, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	prefix := rr.prefixPath()

	getRes, err := rr.kv.Get(ctx, prefix, etcdv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	currentEvents := make([]*WatchEvent, 0, len(getRes.Kvs))
	for _, kv := range getRes.Kvs {
		reg, err := rr.unmarshalRegistration(kv)
		if err != nil {
			return nil, nil, err
		}
		wev := &WatchEvent{
			Key:  string(kv.Key),
			Reg:  reg,
			Type: Create,
		}
		currentEvents = append(currentEvents, wev)
	}

	// Channel to publish registry changes.
	watchEvents := make(chan *WatchEvent)

	// Write a change or exit the watcher.
	put := func(we *WatchEvent) {
		select {
		case <-ctx.Done():
			return
		case watchEvents <- we:
		}
	}
	putTerminalError := func(we *WatchEvent) {
		go func() {
			defer func() {
				recover()
			}()
			select {
			case <-time.After(10 * time.Minute):
			case watchEvents <- we:
			}
		}()
	}
	// Create a watch-event from an event.
	createWatchEvent := func(ev *etcdv3.Event) *WatchEvent {
		wev := &WatchEvent{Key: string(ev.Kv.Key)}
		if ev.IsCreate() {
			wev.Type = Create
		} else if ev.IsModify() {
			wev.Type = Modify
		} else {
			wev.Type = Delete
			// Create base registration from just key.
			reg := &Registration{}
			graphType, graphName, err := rr.graphTypeAndNameFromKey(string(ev.Kv.Key))
			if err != nil {
				fmt.Println(err)
			}
			reg.Type = graphType
			reg.Name = graphName
			wev.Reg = reg
			// Need to return now because
			// delete events don't contain
			// any data to unmarshal.
			return wev
		}
		reg, err := rr.unmarshalRegistration(ev.Kv)
		if err != nil {
			wev.Error = fmt.Errorf("%v: failed unmarshaling value: '%s'", err, ev.Kv.Value)
		} else {
			wev.Reg = reg
		}
		return wev
	}

	// Watch deltas in etcd, with the give prefix, starting
	// at the revision of the get call above.
	deltas := rr.client.Watch(ctx, prefix, etcdv3.WithPrefix(), etcdv3.WithRev(getRes.Header.Revision+1))
	go func() {
		defer close(watchEvents)
		for {
			delta, open := <-deltas
			if !open {
				select {
				case <-ctx.Done():
				default:
					putTerminalError(&WatchEvent{Error: ErrWatchClosedUnexpectedly})
				}
				return
			}
			if delta.Err() != nil {
				putTerminalError(&WatchEvent{Error: delta.Err()})
				return
			}
			for _, event := range delta.Events {
				put(createWatchEvent(event))
			}
		}
	}()

	return currentEvents, watchEvents, nil
}

// State of the graph, either wanted or actual.
type State string

const (
	// Unknown state.
	Unknown State = "unknown"
	// Running state.
	Running State = "running"
	// Stopping state, graceful stop.
	Stopping State = "stopping"
	// Terminating state, drop everything on the floor stop.
	Terminating State = "terminating"
)

// Insert the graph entry.
func (rr *Registry) Insert(ctx context.Context, graphType, graphName string, state State, config []byte) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	key := rr.keyFromGraphTypeAndName(graphType, graphName)

	getRes, err := rr.kv.Get(ctx, key, etcdv3.WithLimit(2))
	if err != nil {
		return err
	}

	if getRes.Count > 1 {
		return ErrMultipleValues
	}
	if getRes.Count > 0 {
		return ErrAlreadyInserted
	}

	return rr.insert(ctx, 0, key, &Registration{
		Type:   graphType,
		Name:   graphName,
		Wanted: string(state),
		Config: base64.StdEncoding.EncodeToString(config),
	})
}

// Select the graph entry.
func (rr *Registry) Select(ctx context.Context, graphType, graphName string) (*Registration, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	key := rr.keyFromGraphTypeAndName(graphType, graphName)

	getRes, err := rr.kv.Get(ctx, key, etcdv3.WithLimit(2))
	if err != nil {
		return nil, err
	}

	if getRes.Count == 0 {
		return nil, ErrZeroValues
	}
	if getRes.Count > 1 {
		return nil, ErrMultipleValues
	}

	return rr.unmarshalRegistration(getRes.Kvs[0])
}

// Delete the graph entry.
func (rr *Registry) Delete(ctx context.Context, graphType, graphName string) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	key := rr.keyFromGraphTypeAndName(graphType, graphName)

	getRes, err := rr.kv.Get(ctx, key, etcdv3.WithLimit(2))
	if err != nil {
		return err
	}

	if getRes.Count == 0 {
		return ErrAlreadyDeleted
	}
	if getRes.Count > 1 {
		return ErrMultipleValues
	}

	return rr.delete(ctx, getRes.Kvs[0].Version, key)
}

// SetWanted state of the graph under the given namespace.
func (rr *Registry) SetWanted(ctx context.Context, graphType, graphName string, state State) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	key := rr.keyFromGraphTypeAndName(graphType, graphName)

	getRes, err := rr.kv.Get(ctx, key, etcdv3.WithLimit(2))
	if err != nil {
		return err
	}

	if getRes.Count == 0 {
		return nil
	}
	if getRes.Count > 1 {
		return ErrMultipleValues
	}

	kv := getRes.Kvs[0]
	rec := &Registration{}
	err = json.Unmarshal(kv.Value, rec)
	if err != nil {
		return err
	}
	if rec.Wanted == string(state) {
		return nil
	}
	rec.Wanted = string(state)

	return rr.insert(ctx, getRes.Kvs[0].Version, key, rec)
}

func (rr *Registry) logf(format string, v ...interface{}) {
	if rr.Logger != nil {
		rr.Logger.Printf(format, v...)
	}
}

func (rr *Registry) insert(ctx context.Context, version int64, key string, reg *Registration) error {
	bytes, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	txnRes, err := rr.kv.Txn(ctx).
		If(etcdv3.Compare(etcdv3.Version(key), "=", version)).
		Then(etcdv3.OpPut(key, string(bytes))).
		Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		return ErrFailedInsert
	}
	return nil
}

func (rr *Registry) delete(ctx context.Context, version int64, key string) error {
	txnRes, err := rr.kv.Txn(ctx).
		If(etcdv3.Compare(etcdv3.Version(key), "=", version)).
		Then(etcdv3.OpDelete(key)).
		Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		return ErrFailedDelete
	}
	return nil
}

func (rr *Registry) prefixPath() string {
	return fmt.Sprintf("flo.%v.graph.", rr.namespace)
}

func (rr *Registry) keyFromGraphTypeAndName(graphType, graphName string) string {
	return fmt.Sprintf("flo.%v.graph.%v.%v", rr.namespace, graphType, graphName)
}

func (rr *Registry) graphTypeAndNameFromKey(key string) (string, string, error) {
	prefix := fmt.Sprintf("flo.%v.graph.", rr.namespace)
	suffix := key[len(prefix):]
	parts := strings.Split(suffix, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key: " + key)
	}
	return parts[0], parts[1], nil
}

func (rr *Registry) unmarshalRegistration(kv *mvccpb.KeyValue) (*Registration, error) {
	rec := &Registration{}
	err := json.Unmarshal(kv.Value, rec)
	if err != nil {
		return nil, err
	}
	graphType, graphName, err := rr.graphTypeAndNameFromKey(string(kv.Key))
	if err != nil {
		return nil, err
	}
	rec.Type = graphType
	rec.Name = graphName
	return rec, nil
}
