package driver

// Cfg of storage.
type Cfg interface {
	Driver() string
}
