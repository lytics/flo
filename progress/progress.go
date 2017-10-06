package progress

import "time"

type EventTime struct {
	Graph string
	Time  time.Time
}

type SourceDone struct {
	Graph  string
	Source string
}

type Heuristic struct {
	EOS bool
}
