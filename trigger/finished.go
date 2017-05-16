package trigger

import (
	"log"
	"os"

	"github.com/lytics/flo/window"
)

func WhenFinished() *Finished {
	return &Finished{
		logger: log.New(os.Stderr, "finished-trigger: ", log.LstdFlags),
	}
}

type Finished struct {
	delta  bool
	logger *log.Logger
}

func (t *Finished) Modified(key string, v interface{}, vs map[window.Span][]interface{}) error {
	return nil
}

func (t *Finished) Start(signal func(keys []string)) {}

func (t *Finished) Stop() {}

func (t *Finished) Delta() *Finished {
	t.delta = true
	return t
}
