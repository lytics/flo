package slowdown

import (
	"context"

	"github.com/lytics/flo"
	"golang.org/x/time/rate"
)

// Wrap a collection to slow it down to the give
// events-per-second for calls to Next.
func Wrap(eps float64, vs flo.Collection) *Slowdown {
	return &Slowdown{
		vs:      vs,
		limiter: rate.NewLimiter(rate.Limit(eps), 2),
	}
}

// Slowdown a collection.
type Slowdown struct {
	vs      flo.Collection
	limiter *rate.Limiter
}

// Next item in the collection.
func (s *Slowdown) Next(ctx context.Context) (string, interface{}, error) {
	s.limiter.Wait(ctx)
	return s.vs.Next(ctx)
}

// Name of the collection.
func (s *Slowdown) Name() string {
	return s.vs.Name()
}

// Init the collection.
func (s *Slowdown) Init() error {
	return s.vs.Init()
}

// Close the connection.
func (s *Slowdown) Close() error {
	return s.vs.Close()
}
