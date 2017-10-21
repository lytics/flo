package window

import (
	"testing"
	"time"
)

func TestAllWindow(t *testing.T) {
	ts := time.Date(2017, 01, 01, 13, 47, 1, 0, time.UTC)

	// All-of-time window. Regardless of which timestamp
	// this window receives it buckets the timestamp
	// into the single universal window of time.
	all := All()

	ss := newState()
	all.Merge(ts, items(0), ss, appendMerge)

	// The expected window of time is the single
	// universal window.
	expected := NewSpan(time.Date(01, 01, 01, 0, 0, 0, 0, time.UTC), time.Date(292277026596, 1, 1, 0, 0, 0, 0, time.UTC))

	vs := ss.Get(expected)
	if vs == nil {
		t.Fatal("expected to find window:", expected)
	}
	if vs[0].(int) != 0 {
		t.Fatal("expected data value")
	}
}
