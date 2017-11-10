package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/lytics/flo/storage/driver"
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
func (db *DB) Apply(ctx context.Context, key string, mut driver.Mutation) error {
	return db.conn.Apply(ctx, key, mut)
}

// Drain the keys into the sink.
func (db *DB) Drain(ctx context.Context, keys []string, sink driver.Sink) error {
	return db.conn.Drain(ctx, keys, sink)
}
