package trigger

import (
	"log"
	"os"

	"github.com/lytics/flo/progress"
	"github.com/lytics/flo/window"
)

// AtCount count, in per key events, emit changes.
func AtCount(count int) *Count {
	return &Count{
		count:    count,
		modified: map[string]int{},
		logger:   log.New(os.Stderr, "count-trigger: ", log.LstdFlags),
	}
}

type Count struct {
	signal   func([]string)
	count    int
	delta    bool
	logger   *log.Logger
	modified map[string]int
}

func (t *Count) Heuristic(*progress.Heuristic) {}

func (t *Count) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	current := t.modified[key]
	if current >= t.count {
		t.signal([]string{key})
		t.modified[key] = 0
	} else {
		t.modified[key] = current + 1
	}
	return nil
}

func (t *Count) Start(signal func(keys []string)) error {
	t.signal = signal
	return nil
}

func (t *Count) Stop() {}

func (t *Count) Delta() *Count {
	t.delta = true
	return t
}
