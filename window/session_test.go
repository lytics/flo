package window

import (
	"testing"
	"time"
)

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
	ss := newState()
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
		vs := ss.Get(s.Session)
		if vs == nil {
			t.Fatalf("expected to find window: %v", s)
		}
		if !equal(s.Data, vs) {
			t.Fatal()
		}
	}
}
