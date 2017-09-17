package source

// WrapSources from a collection of sources.
func WrapSources(ss ...Source) *WrappedSources {
	return nil
}

// WrappedSources that implement a no-op setup.
type WrappedSources struct {
	ss []Source
}

// Setup the sources.
func (ws *WrappedSources) Setup(graphType, graphName string, conf []byte) ([]Source, error) {
	return nil, nil
}
