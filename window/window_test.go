package window

import (
	"testing"
	"time"
)

func appendMerge(a, b []interface{}) ([]interface{}, error) {
	c := append(a, b...)
	return c, nil
}

func TestSlidingWindow(t *testing.T) {
	// The sliding window should bucket this single timestamp
	// into two windows of time when the width of the window
	// is 5 minutes, and the period is 2 minutes.
	ts := time.Date(2017, 01, 01, 13, 47, 1, 0, time.UTC)

	// Sliding window, where each window is 5 minutes wide
	// and produced at a period of 2 minutes.
	sliding := Sliding(5*time.Minute, 2*time.Minute)

	ss := map[Span][]interface{}{}
	sliding.Merge(ts, items(0), ss, appendMerge)

	// Check that both expected windows were produced
	// from the timestamp.
	expected := []Span{
		NewSpan(time.Date(2017, 01, 01, 13, 45, 0, 0, time.UTC), time.Date(2017, 01, 01, 13, 50, 0, 0, time.UTC)),
		NewSpan(time.Date(2017, 01, 01, 13, 47, 0, 0, time.UTC), time.Date(2017, 01, 01, 13, 52, 0, 0, time.UTC)),
	}

	for _, s := range expected {
		vs, ok := ss[s]
		if !ok {
			t.Fatal("expected to find window:", s)
		}
		if vs[0].(int) != 0 {
			t.Fatal("expected data value")
		}
	}
}

func TestAllWindow(t *testing.T) {
	ts := time.Date(2017, 01, 01, 13, 47, 1, 0, time.UTC)

	// All-of-time window. Regardless of which timestamp
	// this window receives it buckets the timestamp
	// into the single universal window of time.
	all := All()

	ss := map[Span][]interface{}{}
	all.Merge(ts, items(0), ss, appendMerge)

	// The expected window of time is the single
	// universal window.
	expected := NewSpan(time.Date(01, 01, 01, 0, 0, 0, 0, time.UTC), time.Date(292277026596, 1, 1, 0, 0, 0, 0, time.UTC))

	vs, ok := ss[expected]
	if !ok {
		t.Fatal("expected to find window:", expected)
	}
	if vs[0].(int) != 0 {
		t.Fatal("expected data value")
	}
}

func TestSessionWindow(t *testing.T) {
	// These event times should produce three sessions,
	// where the session timeout is 30 minutes.
	times := []time.Time{
		time.Date(2017, 02, 17, 13, 1, 0, 0, time.UTC), // Session A
		time.Date(2017, 02, 17, 13, 2, 0, 0, time.UTC), // Session A
		time.Date(2017, 02, 17, 15, 3, 0, 0, time.UTC), // Session B
		time.Date(2017, 02, 18, 15, 4, 0, 0, time.UTC), // Session C
		time.Date(2017, 02, 18, 15, 5, 0, 0, time.UTC), // Session C
	}

	// Session with timeout of 30 minutes.
	session := Session(30 * time.Minute)

	// Merge in the sequence of events. Here nil
	// data is used, the intent is to check that
	// the sessions windows are evaluated
	// correctly.
	ss := map[Span][]interface{}{}
	for i, ts := range times {
		err := session.Merge(ts, items(i), ss, appendMerge)
		if err != nil {
			t.Fatal(err)
		}
	}

	// The events should have been merged
	// down into three sessions.
	expected := []struct {
		Session Span
		Data    []interface{}
	}{
		{
			Session: NewSpan(time.Date(2017, 02, 17, 13, 1, 0, 0, time.UTC), time.Date(2017, 02, 17, 13, 32, 0, 0, time.UTC)),
			Data:    []interface{}{item(0), item(1)},
		},
		{
			Session: NewSpan(time.Date(2017, 02, 17, 15, 3, 0, 0, time.UTC), time.Date(2017, 02, 17, 15, 33, 0, 0, time.UTC)),
			Data:    []interface{}{item(2)},
		},
		{
			Session: NewSpan(time.Date(2017, 02, 18, 15, 4, 0, 0, time.UTC), time.Date(2017, 02, 18, 15, 35, 0, 0, time.UTC)),
			Data:    []interface{}{item(3), item(4)},
		},
	}

	// Check that each expected session actually exists.
	for _, s := range expected {
		vs, ok := ss[s.Session]
		if !ok {
			t.Fatalf("expected to find window: %v", s)
		}
		if !equal(s.Data, vs) {
			t.Fatal()
		}
	}
}

func items(i int) []interface{} {
	return []interface{}{i}
}

func item(i int) interface{} {
	return i
}

func equal(vs, ws []interface{}) bool {
	if len(vs) != len(ws) {
		return false
	}

	contains := func(v interface{}, bag []interface{}) bool {
		for _, b := range bag {
			if v.(int) == b.(int) {
				return true
			}
		}
		return false
	}

	for _, v := range vs {
		if !contains(v, ws) {
			return false
		}
	}
	return true
}

func clock(t time.Time) string {
	return t.Format("15:04:05")
}
