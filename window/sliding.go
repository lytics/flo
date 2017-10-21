package window

import (
	"time"

	"github.com/lytics/flo/merger"
)

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
func (w *sliding) Merge(ts time.Time, vs []interface{}, ss State, f merger.ManyMerger) error {
	for _, tss := range w.Apply(ts) {
		vs0 := ss.Get(tss)
		vs2, err := f(vs, vs0)
		if err != nil {
			return err
		}
		ss.Set(tss, vs2)
	}
	return nil
}
