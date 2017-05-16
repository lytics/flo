package trigger

import "github.com/lytics/flo/window"

// Trigger an action.
type Trigger interface {
	Modified(key string, v interface{}, vs map[window.Span][]interface{}) error
	Start(func(keys []string))
	Stop()
}
