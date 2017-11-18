package trigger

import (
	"github.com/lytics/flo/progress"
	"github.com/lytics/flo/window"
)

// Trigger an action.
type Trigger interface {
	Heuristic(*progress.Heuristic)
	Modified(key string, v interface{}, vs map[window.Span][]interface{}) error
	Start(func(keys []string) error) error
	Stop()
}
