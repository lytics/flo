package source

// SkipSetup for a collection of sources.
func SkipSetup(ss ...Source) *SkipSetupSources {
	return &SkipSetupSources{
		ss: ss,
	}
}

// SkipSetupSources implements a no-op setup.
type SkipSetupSources struct {
	ss []Source
}

// Setup the sources, but really do nothing.
func (ws *SkipSetupSources) Setup(graphType, graphName string, conf []byte) ([]Source, error) {
	return ws.ss, nil
}
