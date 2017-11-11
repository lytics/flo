package boltdriver

import (
	"os"

	"github.com/boltdb/bolt"
)

// Cfg for Bolt database.
type Cfg struct {
	BaseDir  string
	Options  *bolt.Options
	FileMode os.FileMode
}

// Driver name.
func (c Cfg) Driver() string {
	return DriverName
}
