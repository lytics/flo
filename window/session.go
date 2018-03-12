package window

import (
	"time"

	"github.com/lytics/flo/merger"
)

// Session window based on a last activity timeout.
func Session(timeout time.Duration) Window {
	return &session{
		timeout: timeout,
	}
}

type session struct {
	timeout time.Duration
}

func (w *session) apply(ts time.Time) Span {
	// Truncated timestamp.
	tts := ts.Truncate(1 * time.Minute)
	// Min and max of session window.
	min := tts
	max := tts.Add(w.timeout)
	return NewSpan(min, max)
}

func (w *session) Apply(ts time.Time) []Span {
	return []Span{w.apply(ts)}
}

// Merge the new value v into the appropriate existing
// windows in previous state, possibly expanding some
// existing windows.
func (w *session) Merge(s Span, v interface{}, prev State, f merger.ManyMerger) error {
	var err error

	// Check each existing window, and if it
	// overlaps with the new window 's'
	// merge the two together along with
	// the data.
	vs := []interface{}{v}
	rs := []Span{}
	for _, s0 := range prev.Spans() {
		vs0 := prev.Get(s0)
		if s.Overlap(s0) {
			// Merge new data with existing
			// data for overlapping windows.
			vs, err = f(vs, vs0)
			if err != nil {
				return err
			}
			// Mark old window for removal.
			rs = append(rs, s0)
			// Expand window, it will
			// overlap removed window.
			s = s.Expand(s0)
		}
	}

	// Put the new possibly expanded and
	// merged session into the map.
	prev.Coalesce(s, rs, vs)

	// Call it good.
	return nil
}
