package storage

import (
	"context"
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
func Open(name string, cfg driver.Cfg) (*DB, error) {
	drvr, ok := drivers[cfg.Driver()]
	if !ok {
		return nil, fmt.Errorf("storage: unknown driver: %v", cfg.Driver())
	}
	conn, err := drvr.Open(name, cfg)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to open: %v", err)
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
func (db *DB) Apply(ctx context.Context, key string, mut driver.Mutation) (map[window.Span]driver.Update, error) {
	return db.conn.Apply(ctx, key, mut)
}
