package slowdown

import (
	"context"

	"github.com/lytics/flo/source"
	"golang.org/x/time/rate"
)

// Wrap a source to slow it down to the given
// events-per-second for calls to Next.
func Wrap(eps float64, vs source.Source) *Slowdown {
	return &Slowdown{
		vs:      vs,
		limiter: rate.NewLimiter(rate.Limit(eps), 2),
	}
}

// Slowdown a source.
type Slowdown struct {
	vs      source.Source
	limiter *rate.Limiter
}

// Next item in the source.
func (s *Slowdown) Next(ctx context.Context, put source.Put) error {
	s.limiter.Wait(ctx)
	return s.vs.Next(ctx, put)
}

// Metadata of the source.
func (s *Slowdown) Metadata() source.Metadata {
	return s.vs.Metadata()
}

// Init the source.
func (s *Slowdown) Init() error {
	return s.vs.Init()
}

// Stop the source.
func (s *Slowdown) Stop() error {
	return s.vs.Stop()
}
