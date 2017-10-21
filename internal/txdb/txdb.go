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
	mu       sync.Mutex
	dataType string
	Windows  map[window.Span][]interface{}
}

func (r *Row) Del(k window.Span) {
	delete(r.Windows, k)
}

func (r *Row) Get(k window.Span) []interface{} {
	return r.Windows[k]
}

func (r *Row) Set(k window.Span, v []interface{}) {
	r.Windows[k] = v
}

func (r *Row) Spans() []window.Span {
	ss := make([]window.Span, 0, len(r.Windows))
	for k := range r.Windows {
		ss = append(ss, k)
	}
	return ss
}

func (r *Row) Snapshot() *Row {
	s := newRow()
	for k, v := range r.Windows {
		s.Windows[k] = v
	}

	return s
}

func New(name string) *DB {
	return &DB{
		buckets: map[string]*Bucket{},
	}
}

type DB struct {
	mu      sync.Mutex
	buckets map[string]*Bucket
}

func (db *DB) Bucket(name string) *Bucket {
	db.mu.Lock()
	defer db.mu.Unlock()

	b, ok := db.buckets[name]
	if !ok {
		b = NewBucket(name)
		db.buckets[name] = b
	}
	return b
}

// NewBucket in the database.
func NewBucket(name string) *Bucket {
	return &Bucket{
		values: map[string]*Row{},
	}
}

// Bucket of key values organized into individual
// windows of time.
type Bucket struct {
	mu     sync.Mutex
	name   string
	values map[string]*Row
}

// Drain the keys into the sink.
func (m *Bucket) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {
	for key, row := range m.snapshot(keys) {
		for span, vs := range row.Windows {
			sink(span, key, vs)
		}
	}
}

// Apply the mutation to the graph key's row.
func (m *Bucket) Apply(key string, mutation func(*Row) error) error {
	row := m.fetchRowForMutation(key)

	row.mu.Lock()
	defer row.mu.Unlock()

	return mutation(row)
}

func (m *Bucket) fetchRowForMutation(key string) *Row {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the row for this key.
	row, ok := m.values[key]
	if !ok {
		row = newRow()
		m.values[key] = row
	}
	return row
}

func (m *Bucket) snapshot(keys []string) map[string]*Row {
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
