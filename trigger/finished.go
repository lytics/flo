package trigger

import (
	"log"
	"os"
	"sync"

	"github.com/lytics/flo/progress"
	"github.com/lytics/flo/window"
)

func WhenFinished() *Finished {
	return &Finished{
		logger:   log.New(os.Stderr, "finished-trigger: ", log.LstdFlags),
		modified: map[string]bool{},
	}
}

type Finished struct {
	mu       sync.Mutex
	logger   *log.Logger
	signal   func([]string)
	modified map[string]bool
}

func (t *Finished) Heuristic(h *progress.Heuristic) {
	if !h.EOS {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.modified) == 0 {
		return
	}

	keys := []string{}
	for key := range t.modified {
		keys = append(keys, key)
	}
	t.modified = map[string]bool{}

	t.signal(keys)
}

func (t *Finished) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.modified[key] = true
	return nil
}

func (t *Finished) Start(signal func(keys []string)) {
	t.signal = signal
}

func (t *Finished) Stop() {}
