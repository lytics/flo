package trigger

import (
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

// NewHeuristic for the named graph.
func NewHeuristic(graph string) *Heuristic {
	return &Heuristic{
		Graph:     graph,
		EventTime: metrics.NewHistogram(metrics.NewExpDecaySample(10000, 0.015)),
	}
}

// Heuristic to help discover when in event
// time a graph is currently processing.
type Heuristic struct {
	Graph     string            // Name of graph.
	EOS       bool              // End Of Streams.
	EventTime metrics.Histogram // Event time last 15 minutes.
}

// Event time update.
func (h *Heuristic) Event(ts time.Time) {
	h.EventTime.Update(ts.Unix())
}
