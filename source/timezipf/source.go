package timezipf

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

type Event struct {
	TS    time.Time
	Value string
}

func New(s float64, alphabet []string) *TimeZipf {
	uniq := map[string]bool{}
	for _, a := range alphabet {
		uniq[a] = true
	}
	sorted := []string{}
	for a := range uniq {
		sorted = append(sorted, a)
	}
	sort.Strings(sorted)

	dice := rand.New(rand.NewSource(0))

	clocks := []time.Time{}
	for range sorted {
		clocks = append(clocks, time.Date(1980, 01, 01, 00, 00, 00, 0, time.UTC))
	}

	return &TimeZipf{
		zipf:     rand.NewZipf(dice, s, 2, uint64(len(sorted)-1)),
		alphabet: sorted,
		clocks:   clocks,
		dice:     rand.New(rand.NewSource(0)),
	}
}

type TimeZipf struct {
	pos      int
	zipf     *rand.Zipf
	alphabet []string
	clocks   []time.Time
	dice     *rand.Rand
}

func (s *TimeZipf) Next(context.Context) (string, interface{}, error) {
	i := int(s.zipf.Uint64())
	id := strconv.Itoa(s.pos)
	s.pos++

	ts := s.clocks[i]
	event := &Event{
		TS:    ts,
		Value: s.alphabet[i],
	}
	s.clocks[i] = s.clocks[i].Add(time.Duration(1+s.dice.Intn(29)) * time.Minute)

	return id, event, nil
}

func (s *TimeZipf) Init() error {
	return nil
}

func (s *TimeZipf) Start(signal func(keys []string) error) error {
	return nil
}

func (s *TimeZipf) Close() error {
	return nil
}
