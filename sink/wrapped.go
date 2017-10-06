package sink

// SkipSetup for a collection of sinks.
func SkipSetup(ss ...Sink) *SkipSetupSinks {
	return &SkipSetupSinks{
		ss: ss,
	}
}

// SkipSetupSinks implements a no-op setup.
type SkipSetupSinks struct {
	ss []Sink
}

// Setup the sinks, but really do nothing.
func (ws *SkipSetupSinks) Setup(graphType, graphName string, conf []byte) ([]Sink, error) {
	return ws.ss, nil
}
