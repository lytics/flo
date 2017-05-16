package window

import (
	"fmt"
	"time"

	"github.com/lytics/flo/merger"
)

// NewSpan from the start and end times.
func NewSpan(start, end time.Time) Span {
	return Span{start.Unix(), end.Unix()}
}

// Span of time defining a window. The first element
// is the start,inclusive, and the second element is
// the end, exclusive. In other words: [start,end)
type Span [2]int64

// Start of the span.
func (s Span) Start() time.Time {
	return time.Unix(s[0], 0)
}

// End of the span.
func (s Span) End() time.Time {
	return time.Unix(s[1], 0)
}

// String of the span.
func (s Span) String() string {
	return fmt.Sprintf("[%v,%v)", time.Unix(s[0], 0), time.Unix(s[1], 0))
}

// Equal when this span and r have the same
// start and end times.
func (s Span) Equal(r Span) bool {
	return s[0] == r[0] && s[1] == r[1]
}

// Expand this span and r into a new span
// that covers both.
func (s Span) Expand(r Span) Span {
	n := Span{s[0], s[1]}
	if r[0] < s[0] {
		n[0] = r[0]
	}
	if r[1] > s[1] {
		n[1] = r[1]
	}
	return n
}

// Overlap returns true when this span and r
// have an overlap.
func (s Span) Overlap(r Span) bool {
	// TRUE IF:
	//     s = [10:13, 10:23)
	//     r =   [10:17, 10:27)
	// OR
	//     s =   [10:17, 10:27)
	//     r = [10:13, 10:23)
	// OR
	//     s = [10:13, 10:23)
	//     r = [10:13, 10:23)
	//
	// FASE OTHERWISE:
	//     s = [10:13, 10:23)
	//     r =        [10:23, 10:33)
	// OR
	//     s =        [10:23, 10:33)
	//     r = [10:13, 10:23)
	if s.Start().Equal(r.Start()) {
		return true
	}
	if s.Start().Before(r.Start()) {
		return r.Start().Before(s.End())
	}
	return s.Start().Before(r.End())
}

// Window strategy.
type Window interface {
	Apply(ts time.Time) []Span
	Merge(ts time.Time, vs []interface{}, ss map[Span][]interface{}, f merger.ManyMerger) error
}

// All of time window, which is considered to be the window of
// time between the dates:
//
//             0001-01-01 00:00:00 +0000 UTC
//     292277026596-01-01 00:00:00 +0000 UTC
//
// Where the max is about 292 billion years in the future.
func All() Window {
	min := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	max := time.Date(292277026596, 1, 1, 0, 0, 0, 0, time.UTC)
	return &all{
		universe: NewSpan(min, max),
	}
}

type all struct {
	universe Span
}

func (w *all) Apply(time.Time) []Span {
	return []Span{w.universe}
}

// Merge the new value vs into the universal window
// in ss.
func (w *all) Merge(ts time.Time, vs []interface{}, ss map[Span][]interface{}, f merger.ManyMerger) error {
	vs0 := ss[w.universe]
	vs2, err := f(vs, vs0)
	if err != nil {
		return err
	}
	ss[w.universe] = vs2
	return nil
}

// Sliding window, with a width and period. When
// the period is smaller than the width, multiple
// windows are produced for each timestamp.
func Sliding(width, period time.Duration) Window {
	return &sliding{width: width, period: period}
}

type sliding struct {
	width  time.Duration
	period time.Duration
}

func (w *sliding) Apply(ts time.Time) []Span {
	// Truncated timestamp.
	tts := ts.Truncate(1 * time.Minute).Truncate(w.width)
	// Min and max of window.
	min := tts
	max := tts.Add(w.width)

	// When the period and width are of
	// different sizes, ie: the period
	// is smaller than the width,
	// multiple windows are produced.
	var ws []Span
	t0 := min
	t1 := max
	for {
		if (ts.Equal(t0) || ts.After(t0)) && ts.Before(t1) {
			ws = append(ws, NewSpan(t0, t1))
		}
		t0 = t0.Add(w.period)
		t1 = t1.Add(w.period)
		if t0.After(ts) {
			break
		}
	}
	return ws
}

// Merge the new value vs into the appropriate existing windows
// found in ss.
func (w *sliding) Merge(ts time.Time, vs []interface{}, ss map[Span][]interface{}, f merger.ManyMerger) error {
	for _, tss := range w.Apply(ts) {
		vs0 := ss[tss]
		vs2, err := f(vs, vs0)
		if err != nil {
			return err
		}
		ss[tss] = vs2
	}
	return nil
}

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

// Merge the new value vs into the appropriate existing
// windows in ss, possibly expanding existing windows.
func (w *session) Merge(ts time.Time, vs []interface{}, ss map[Span][]interface{}, f merger.ManyMerger) error {
	var err error

	// Create the session window.
	s := w.apply(ts)

	// Check each existing window, and if it
	// overlaps with the new window 's'
	// merge the two together along with
	// the data.
	remove := map[Span]bool{}
	for s0, vs0 := range ss {
		if s.Overlap(s0) {
			// Merge new data with existing
			// data for overlapping windows.
			vs, err = f(vs, vs0)
			if err != nil {
				return err
			}
			// Mark old window for removal.
			remove[s0] = true
			// Expand window, it will
			// overlap removed window.
			s = s.Expand(s0)
		}
	}

	// Remove the old sessions windows
	// that have been merged, since
	// they are no longer valid.
	for s0 := range remove {
		delete(ss, s0)
	}

	// Put the new possibly expanded and
	// merged session into the map.
	ss[s] = vs

	// Call it good.
	return nil
}

func expandToElements() {
	// Darren -> Dar-eng

	// 	Expands per-key, per-window
	// groups of values into (key, value, event time, window)
	// tuples, with new per-window timestamps. In this example,
	// we set the timestamp to the end of the window,
	// but any timestamp greater than or equal to the timestamp
	// of the earliest event in the window is valid with
	// respect to watermark correctness
}
