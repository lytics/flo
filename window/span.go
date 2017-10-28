package window

import (
	"encoding/binary"
	"fmt"
	"time"
)

var (
	zero = Span{}
)

// NewSpan from the start and end times.
func NewSpan(start, end time.Time) Span {
	return Span{start.Unix(), end.Unix()}
}

// NewSpanFromKey produced by a previous call to Key.
func NewSpanFromKey(key []byte) (Span, error) {
	if key[8] != ':' {
		return zero, fmt.Errorf("span: not a valid span key")
	}
	t0 := int64(binary.BigEndian.Uint64(key[0:8]))
	t1 := int64(binary.BigEndian.Uint64(key[9:17]))
	if t0 < 0 {
		return zero, fmt.Errorf("span: negative time value")
	}
	if t1 < 0 {
		return zero, fmt.Errorf("span: negative time value")
	}
	return Span{t0, t1}, nil
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

// Key of span usable as a datastore identifier.
func (s Span) Key() ([]byte, error) {
	if s[0] < 0 {
		return nil, fmt.Errorf("span: negative time value")
	}
	if s[1] < 0 {
		return nil, fmt.Errorf("span: negative time value")
	}
	key := make([]byte, 8+8+1)
	binary.BigEndian.PutUint64(key[0:], uint64(s[0]))
	key[8] = ':'
	binary.BigEndian.PutUint64(key[9:], uint64(s[1]))
	return key, nil
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
	// FASE IF:
	//     s = [10:13, 10:23)
	//     r =        [10:23, 10:33)
	// OR
	//     s =        [10:23, 10:33)
	//     r = [10:13, 10:23)
	// OR
	//     s =                 [10:30, 10:33)
	//     r = [10:13, 10:20)
	if s.Start().Equal(r.Start()) {
		return true
	}
	if s.Start().Before(r.Start()) {
		return r.Start().Before(s.End())
	}
	return s.Start().Before(r.End())
}
