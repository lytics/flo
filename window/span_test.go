package window

import (
	"testing"
	"time"
)

func TestSpanKey(t *testing.T) {
	t0 := time.Now()

	s0 := NewSpan(t0, t0.Add(10*time.Minute))
	k0, err := s0.Key()
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewSpanFromKey(k0)
	if err != nil {
		t.Fatal(err)
	}
	if !s0.Equal(s1) {
		t.Fatalf("expected spans to be equal: %v != %v", s0, s1)
	}
}
