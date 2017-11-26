package linefile

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/lytics/flo/source"
)

// FromFile create a source.
func FromFile(name string) *Source {
	return &Source{
		meta: source.Metadata{
			Name:       name,
			Addressing: source.Sequential,
		},
	}
}

// Source of data.
type Source struct {
	mu   sync.Mutex
	f    *os.File
	r    *bufio.Reader
	pos  int
	meta source.Metadata
}

// Metadata about the source.
func (s *Source) Metadata() source.Metadata {
	return s.meta
}

// Init the source.
func (s *Source) Init(ctx context.Context, checkpoint interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ok bool
	var cp *Checkpoint

	if checkpoint != nil {
		cp, ok = checkpoint.(*Checkpoint)
		if !ok {
			return fmt.Errorf("linefile: checkpoint must be of type linefile.Checkpoint, not: %T", checkpoint)
		}
		if cp.Name != s.meta.Name {
			return fmt.Errorf("linefile: checkpoint name: %v, different from source name: %v", cp.Name, s.meta.Name)
		}
	}

	if s.f != nil {
		return nil
	}

	f, err := os.Open(s.meta.Name)
	if err != nil {
		return err
	}
	s.f = f
	s.r = bufio.NewReader(s.f)

	if ok {
		for int64(s.pos) < cp.Pos {
			_, err := s.take(ctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Stop the source.
func (s *Source) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return nil
	}

	f := s.f
	s.f = nil

	return f.Close()
}

// Take next value from source.
func (s *Source) Take(ctx context.Context) (*source.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, context.DeadlineExceeded
	default:
	}

	return s.take(ctx)
}

// take without locking.
func (s *Source) take(ctx context.Context) (*source.Item, error) {
	v, err := s.r.ReadString('\n')
	if err != nil {
		return nil, err
	}

	item := source.NewItem(v, &Checkpoint{
		Name: s.meta.Name,
		Pos:  int64(s.pos),
	}, nil)

	s.pos++

	return item, nil
}
