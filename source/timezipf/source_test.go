package timezipf

import (
	"context"
	"testing"
)

func TestTimeZipf(t *testing.T) {
	s := New(3.0, []string{"A", "B", "C", "D"})
	ctx := context.Background()

	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		_, v, _ := s.Next(ctx)
		counts[v.(*Event).Value]++
	}

	// The collection should be deterministic, every
	// run for a given alphabet should produce the
	// same values.
	if counts["A"] != 690 {
		t.Fatal("expected", 690)
	}
	if counts["B"] != 200 {
		t.Fatal("expected", 200)
	}
	if counts["C"] != 67 {
		t.Fatal("expected", 67)
	}
	if counts["D"] != 43 {
		t.Fatal("expected", 43)
	}
}
