package trigger

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/lytics/flo/progress"
	"github.com/lytics/flo/window"
)

// AtPeriod period, in processing time, emit changes.
func AtPeriod(period time.Duration) *Period {
	return &Period{
		stop:     make(chan bool),
		delta:    false,
		period:   period,
		modified: map[string]bool{},
		logger:   log.New(os.Stderr, "period-trigger: ", log.LstdFlags),
	}
}

type Period struct {
	mu       sync.Mutex
	stop     chan bool
	delta    bool
	period   time.Duration
	logger   *log.Logger
	modified map[string]bool
}

func (t *Period) Heuristic(*progress.Heuristic) {}

func (t *Period) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.modified[key] = true
	return nil
}

func (t *Period) Start(signal func(keys []string)) error {
	ticker := time.NewTicker(t.period)
	defer ticker.Stop()

	snapshot := func() []string {
		t.mu.Lock()
		defer t.mu.Unlock()

		keys := []string{}
		for key := range t.modified {
			keys = append(keys, key)
		}
		t.modified = map[string]bool{}

		return keys
	}

	for {
		select {
		case <-t.stop:
			return nil
		case <-ticker.C:
			signal(snapshot())
		}
	}
}

func (t *Period) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stop:
		return
	default:
		close(t.stop)
	}
}

func (t *Period) Delta() *Period {
	t.delta = true
	return t
}
