package txdb

import (
	"sync"

	"github.com/lytics/flo/window"
)

func newRow() *Row {
	return &Row{
		Windows: map[window.Span][]interface{}{},
	}
}

// Row data for a key.
type Row struct {
	mu      sync.Mutex
	Windows map[window.Span][]interface{}
}

func (r *Row) Snapshot() *Row {
	s := newRow()
	for k, v := range r.Windows {
		s.Windows[k] = v
	}

	return s
}

// New database.
func New(name string) *DB {
	return &DB{
		values: map[string]*Row{},
	}
}

// DB of key values organized into individual
// windows of time.
type DB struct {
	mu     sync.Mutex
	name   string
	values map[string]*Row
}

// Drain the keys into the sink.
func (m *DB) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {
	for key, row := range m.snapshot(keys) {
		for span, vs := range row.Windows {
			sink(span, key, vs)
		}
	}
}

// Apply the mutation to the graph key's row.
func (m *DB) Apply(key string, mutation func(*Row) error) error {
	row := m.fetchRowForMutation(key)

	row.mu.Lock()
	defer row.mu.Unlock()

	return mutation(row)
}

func (m *DB) fetchRowForMutation(key string) *Row {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the windows for this key.
	row, ok := m.values[key]
	if !ok {
		row = newRow()
		m.values[key] = row
	}
	return row
}

func (m *DB) snapshot(keys []string) map[string]*Row {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotRow := func(row *Row) *Row {
		row.mu.Lock()
		defer row.mu.Unlock()
		return row.Snapshot()
	}

	snap := map[string]*Row{}
	for _, key := range keys {
		r, ok := m.values[key]
		if !ok {
			panic("delta without value: " + key)
		}
		snap[key] = snapshotRow(r)
	}
	return snap
}
