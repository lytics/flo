package trigger

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/lytics/flo/window"
)

// WhenDormant after a given time, measured per key, emit changes.
func WhenDormant(after time.Duration) *Dormant {
	return &Dormant{
		after:    after,
		modified: map[string]time.Time{},
		logger:   log.New(os.Stderr, "dormant-trigger: ", log.LstdFlags),
	}
}

// Dormant data trigger.
type Dormant struct {
	mu       sync.Mutex
	after    time.Duration
	delta    bool
	logger   *log.Logger
	ticker   *time.Ticker
	modified map[string]time.Time
}

// Modified key, v is the incoming data, vs is v merged into previous values.
func (t *Dormant) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.modified[key] = time.Now()
	return nil
}

// Start the trigger, signalling chanegd keys with the signal function.
func (t *Dormant) Start(signal func(keys []string)) {
	freq := t.after / 100
	if freq < 1*time.Second {
		freq = 100 * time.Millisecond
	}
	t.ticker = time.NewTicker(freq)

	snapshot := func(now time.Time) []string {
		t.mu.Lock()
		defer t.mu.Unlock()

		stale := []string{}
		for key, ts := range t.modified {
			if now.Sub(ts) > t.after {
				stale = append(stale, key)
				delete(t.modified, key)
			}
		}

		return stale
	}

	go func() {
		for now := range t.ticker.C {
			signal(snapshot(now))
		}
	}()
}

// Stop the trigger.
func (t *Dormant) Stop() {
	t.ticker.Stop()
}

// Delta of current and previous value should be emitted.
func (t *Dormant) Delta() *Dormant {
	t.delta = true
	return t
}
