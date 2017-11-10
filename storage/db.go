package storage

import (
	"fmt"
	"sync"

	"github.com/lytics/flo/storage/driver"
	"github.com/lytics/flo/window"
)

var (
	driversMu = sync.Mutex{}
	drivers   = map[string]driver.Driver{}
)

// Register a storage driver.
func Register(name string, d driver.Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()

	_, ok := drivers[name]
	if ok {
		panic("storage: driver registered twice: " + name)
	}
	drivers[name] = d
}

// Open a new database connection.
func Open(driverName, sourceName string) (*DB, error) {
	drvr, ok := drivers[driverName]
	if !ok {
		return nil, fmt.Errorf("storage: unknown driver: %v", driverName)
	}
	conn, err := drvr.Open(sourceName)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to open connection: %v", err)
	}
	return &DB{
		conn: conn,
	}, nil
}

// DB handle for interaction with a database.
type DB struct {
	conn driver.Conn
}

// Apply the mutation.
func (db *DB) Apply(key string, mutation func(window.State) error) error {
	return db.conn.Apply(key, mutation)
}

// Drain the keys into the sink.
func (db *DB) Drain(keys []string, sink func(span window.Span, key string, vs []interface{}) error) {
	db.conn.Drain(keys, sink)
}
