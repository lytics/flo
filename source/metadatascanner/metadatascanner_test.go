package metadatascanner

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/lytics/flo/source"
)

type TestCase struct {
	name     string
	events   *Source
	expected *source.Metadata
}

func (tc *TestCase) TestDiscovery(t *testing.T) {
	h, err := tc.events.Metadata()
	if err != nil {
		t.Fatal("expected hints")
	}
	if h.Size != tc.expected.Size {
		t.Fatal("expected num events:", tc.expected.Size)
	}
	if !h.MinTime.Equal(tc.expected.MinTime) {
		t.Fatal("expected min event timestamp:", tc.expected.MinTime)
	}
	if !h.MaxTime.Equal(tc.expected.MaxTime) {
		t.Fatal("expected max event timestamp:", tc.expected.MaxTime)
	}
	if h.TimeOrder != tc.expected.TimeOrder {
		t.Fatal("expected sorted by times:", tc.expected.TimeOrder.String())
	}
}

func TestDiscoverHintsSingle(t *testing.T) {
	for _, tc := range []TestCase{
		{
			name: "1 event",
			expected: &source.Metadata{
				Size:      1,
				MinTime:   time.Unix(1, 0),
				MaxTime:   time.Unix(1, 0),
				TimeOrder: source.Equal,
			},
			events: &Source{
				name: "test",
				events: []Event{
					{
						ID: "0",
						TS: time.Unix(1, 0),
					},
				},
			},
		},
		{
			name: "2 events equal time order",
			expected: &source.Metadata{
				Size:      2,
				MinTime:   time.Unix(1, 0),
				MaxTime:   time.Unix(1, 0),
				TimeOrder: source.Equal,
			},
			events: &Source{
				name: "test",
				events: []Event{
					{
						ID: "0",
						TS: time.Unix(1, 0),
					},
					{
						ID: "1",
						TS: time.Unix(1, 0),
					},
				},
			},
		},
		{
			name: "2 events ascending time order",
			expected: &source.Metadata{
				Size:      2,
				MinTime:   time.Unix(1, 0),
				MaxTime:   time.Unix(2, 0),
				TimeOrder: source.Ascending,
			},
			events: &Source{
				name: "test",
				events: []Event{
					{
						ID: "0",
						TS: time.Unix(1, 0),
					},
					{
						ID: "1",
						TS: time.Unix(2, 0),
					},
				},
			},
		},
		{
			name: "2 events descending time order",
			expected: &source.Metadata{
				Size:      2,
				MinTime:   time.Unix(1, 0),
				MaxTime:   time.Unix(2, 0),
				TimeOrder: source.Descending,
			},
			events: &Source{
				name: "test",
				events: []Event{
					{
						ID: "0",
						TS: time.Unix(2, 0),
					},
					{
						ID: "1",
						TS: time.Unix(1, 0),
					},
				},
			},
		},
		{
			name: "3 events unordered",
			expected: &source.Metadata{
				Size:      3,
				MinTime:   time.Unix(1, 0),
				MaxTime:   time.Unix(3, 0),
				TimeOrder: source.Unordered,
			},
			events: &Source{
				name: "test",
				events: []Event{
					{
						ID: "0",
						TS: time.Unix(2, 0),
					},
					{
						ID: "1",
						TS: time.Unix(1, 0),
					},
					{
						ID: "2",
						TS: time.Unix(3, 0),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.TestDiscovery(t)
		})
	}
}

type Event struct {
	ID  string
	TS  time.Time
	Msg interface{}
}

type Source struct {
	pos    int
	name   string
	events []Event
}

func (s *Source) Metadata() (*source.Metadata, error) {
	var meta *source.Metadata
	var err error
	meta, err = Scan(s, func(v interface{}) (time.Time, error) {
		e := v.(Event)
		return e.TS, nil
	})
	if err != nil {
		return nil, err
	}
	s.Init()
	meta.Name = s.name
	meta.Addressing = source.Sequential
	return meta, nil
}

func (s *Source) Next(ctx context.Context, put source.Put) error {
	if s.pos >= len(s.events) {
		return io.EOF
	}
	e := s.events[s.pos]
	id := source.ID(e.ID)
	err := put(ctx, id, e)
	if err != nil {
		return err
	}
	s.pos++
	return nil
}

func (s *Source) Init() error {
	s.pos = 0
	return nil
}

func (s *Source) Stop() error {
	return nil
}
