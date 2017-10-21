package window

import (
	"testing"
	"time"
)

func TestSlidingWindow(t *testing.T) {
	// The sliding window should bucket this single timestamp
	// into two windows of time when the width of the window
	// is 5 minutes, and the period is 2 minutes.
	ts := time.Date(2017, 01, 01, 13, 47, 1, 0, time.UTC)

	// Sliding window, where each window is 5 minutes wide
	// and produced at a period of 2 minutes.
	sliding := Sliding(5*time.Minute, 2*time.Minute)

	ss := newState()
	sliding.Merge(ts, items(0), ss, appendMerge)

	// Check that both expected windows were produced
	// from the timestamp.
	expected := []Span{
		NewSpan(time.Date(2017, 01, 01, 13, 45, 0, 0, time.UTC), time.Date(2017, 01, 01, 13, 50, 0, 0, time.UTC)),
		NewSpan(time.Date(2017, 01, 01, 13, 47, 0, 0, time.UTC), time.Date(2017, 01, 01, 13, 52, 0, 0, time.UTC)),
	}

	for _, s := range expected {
		vs := ss.Get(s)
		if vs == nil {
			t.Fatal("expected to find window:", s)
		}
		if vs[0].(int) != 0 {
			t.Fatal("expected data value")
		}
	}
}
