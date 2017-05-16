package trigger

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/lytics/flo/window"
)

// AtPeriod period, in processing time, emit changes.
func AtPeriod(period time.Duration) *Period {
	return &Period{
		delta:    false,
		period:   period,
		modified: map[string]bool{},
		logger:   log.New(os.Stderr, "period-trigger: ", log.LstdFlags),
	}
}

type Period struct {
	mu       sync.Mutex
	delta    bool
	period   time.Duration
	logger   *log.Logger
	ticker   *time.Ticker
	modified map[string]bool
}

func (t *Period) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.modified[key] = true
	return nil
}

func (t *Period) Start(signal func(keys []string)) {
	t.ticker = time.NewTicker(t.period)

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

	go func() {
		for range t.ticker.C {
			signal(snapshot())
		}
	}()
}

func (t *Period) Stop() {
	t.ticker.Stop()
}

func (t *Period) Delta() *Period {
	t.delta = true
	return t
}
