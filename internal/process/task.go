package process

import (
	"time"

	"github.com/lytics/flo/source"
)

type Task struct {
	Time time.Time
	Data source.Source
}
