package sink

// WrapSinks from a collection of sinks.
func WrapSinks(ss ...Sink) *WrappedSinks {
	return &WrappedSinks{
		ss: ss,
	}
}

// WrappedSinks that implement a no-op setup.
type WrappedSinks struct {
	ss []Sink
}

// Setup the sinks.
func (ws *WrappedSinks) Setup(graphType, graphName string, conf []byte) ([]Sink, error) {
	return ws.ss, nil
}
