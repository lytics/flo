package registry

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"strconv"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/testetcd"
)

const (
	testGraphType = "graph-type-1"
	testGraphName = "graph-name-1"
	testConfig    = "{}"
)

func TestInsertSelectDelete(t *testing.T) {
	client, r, etcdcleanup := bootstrap(t)
	defer etcdcleanup()
	defer client.Close()

	// Insert.
	timeout, cancel := timeoutContext()
	err := r.Insert(timeout, testGraphType, testGraphName, Unknown, []byte(testConfig))
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Select for existance.
	timeout, cancel = timeoutContext()
	reg, err := r.Select(timeout, testGraphType, testGraphName)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if reg == nil {
		t.Fatal("expected not nil registration")
	}

	// Delete.
	timeout, cancel = timeoutContext()
	err = r.Delete(timeout, testGraphType, testGraphName)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Select to confirm delete.
	timeout, cancel = timeoutContext()
	reg, err = r.Select(timeout, testGraphType, testGraphName)
	cancel()
	if err != ErrZeroValues {
		t.Fatal(err)
	}
	if reg != nil {
		t.Fatal("expected zero registrations")
	}
}

func TestInsertTwice(t *testing.T) {
	client, r, etcdcleanup := bootstrap(t)
	defer etcdcleanup()
	defer client.Close()

	// Insert.
	timeout, cancel := timeoutContext()
	err := r.Insert(timeout, testGraphType, testGraphName, Unknown, []byte(testConfig))
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Insert again.
	timeout, cancel = timeoutContext()
	err = r.Insert(timeout, testGraphType, testGraphName, Unknown, []byte(testConfig))
	cancel()
	if err != ErrAlreadyInserted {
		t.Fatal("expected already inserted error")
	}
}

func TestSetWanted(t *testing.T) {
	client, r, etcdcleanup := bootstrap(t)
	defer etcdcleanup()
	defer client.Close()

	// Insert.
	timeout, cancel := timeoutContext()
	err := r.Insert(timeout, testGraphType, testGraphName, Unknown, []byte(testConfig))
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Select for existance.
	timeout, cancel = timeoutContext()
	reg, err := r.Select(timeout, testGraphType, testGraphName)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if reg == nil {
		t.Fatal("expected not nil registration")
	}
	if reg.Wanted != string(Unknown) {
		t.Fatal("expected 'unknown' wanted state")
	}

	// Set wanted state.
	timeout, cancel = timeoutContext()
	err = r.SetWanted(timeout, testGraphType, testGraphName, Terminating)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Select to confirm new state.
	timeout, cancel = timeoutContext()
	reg, err = r.Select(timeout, testGraphType, testGraphName)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if reg == nil {
		t.Fatal("expected one registration")
	}
	if reg.Wanted != string(Terminating) {
		t.Fatal("expected 'terminating' wanted state")
	}
}

func TestWatch(t *testing.T) {
	client, r, etcdcleanup := bootstrap(t)
	defer etcdcleanup()
	defer client.Close()

	// Insert initial data into the registry.
	initial := map[string]bool{
		"graph-name-1": true,
		"graph-name-2": true,
		"graph-name-3": true,
	}

	for name := range initial {
		timeout, cancel := timeoutContext()
		err := r.Insert(timeout, testGraphType, name, Unknown, []byte(testConfig))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start the watch after the initial insert of data.
	watchStarted := make(chan bool)
	watchAdds := make(map[string]bool)
	watchDels := make(map[string]bool)
	ctx, cancelWatch := context.WithCancel(context.Background())

	current, events, err := r.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range current {
		if !initial[e.Reg.Name] {
			t.Fatalf("missing initial graph name: " + e.Reg.Name)
		}
	}

	go func() {
		close(watchStarted)
		for e := range events {
			switch e.Type {
			case Delete:
				watchDels[e.Reg.Name] = true
			case Create:
				watchAdds[e.Reg.Name] = true
			}
		}
	}()

	<-watchStarted

	// After the watch has started, make new additions
	// to the registry. These additions should be seen
	// by the test's watcher.
	additions := map[string]bool{
		"graph-name-4": true,
		"graph-name-5": true,
	}

	for name := range additions {
		timeout, cancel := timeoutContext()
		err := r.Insert(timeout, testGraphType, name, Unknown, []byte(testConfig))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}

	// Delete all of the initial data. These deletes
	// should be seen by the test's watcher.
	for name := range initial {
		timeout, cancel := timeoutContext()
		err := r.Delete(timeout, testGraphType, name)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}

	// Cancel the test's watcher.
	cancelWatch()

	// Check that the expected deletions happened.
	if len(watchDels) != len(initial) {
		t.Fatalf("failed to watch expected deletions: %v, found: %v", initial, watchDels)
	}
	for k := range initial {
		delete(watchDels, k)
	}
	if len(watchDels) != 0 {
		t.Fatalf("failed to watch expected deletions: %v", initial)
	}

	// Check that the expected additions happened.
	if len(watchAdds) != len(additions) {
		t.Fatalf("failed to watch expected additions: %v, found: %v", additions, watchAdds)
	}
	for k := range additions {
		delete(watchAdds, k)
	}
	if len(watchAdds) != 0 {
		t.Fatalf("failed to watch expected additions: %v", additions)
	}

	etcdcleanup()
}

func bootstrap(t *testing.T) (*etcdv3.Client, *Registry, testetcd.Cleanup) {
	client, cleanup := testetcd.StartAndConnect(t)

	r, err := New(client, "namspace-"+strconv.Itoa(rand.Intn(2000)))
	if err != nil {
		t.Fatal(err)
	}

	return client, r, cleanup
}

func timeoutContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}
