package memdriver

// Cfg for Bolt database.
type Cfg struct{}

// Driver name.
func (c Cfg) Driver() string {
	return DriverName
}
