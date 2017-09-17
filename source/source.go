package source

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// TimeOrder indicates if a source is
// ordered by event time.
type TimeOrder int

const (
	// Unordered within the source.
	Unordered TimeOrder = 0
	// Ascending within the source, such that:
	// Forall e_i < e_i+1, timestamp(e_i) <= timestamp(e_i+1)
	Ascending TimeOrder = 1
	// Descending within the source, such that:
	// Forall e_i < e_i+1, timestamp(e_i) >= timestamp(e_i+1)
	Descending TimeOrder = 2
	// Equal in the source, such that:
	// Forall e_i and e_j, timestamp(e_i) == timestamp(e_j)
	Equal TimeOrder = 4
)

func (to TimeOrder) String() string {
	switch to {
	case Unordered:
		return "unordered"
	case Ascending:
		return "ascending"
	case Descending:
		return "descending"
	case Equal:
		return "equal"
	default:
		panic("unknown time order")
	}
}

// ID of item within the source. Sequential
// sources should have IDs with a total
// ordering and increase monotonically.
type ID string

// NoID when ID is unknown or not defined.
const NoID = ID("")

// Addressing used by the source.
type Addressing int

const (
	// None because the source cannot be addressed
	// for a specific item, these are systems such
	// as PubSub.
	None Addressing = 0
	// Sequential addressing for systems such as data
	// stored sequentially in a file, in Kafka, or
	// accessed via some scanner with a sequential
	// notion of a position. Any item can be addressed
	// but doing so in any fashion except sequentially
	// is expensive.
	Sequential Addressing = 1
)

func (a Addressing) String() string {
	switch a {
	case None:
		return "none"
	case Sequential:
		return "sequential"
	default:
		panic("unknown addressing")
	}
}

// Metadata about the data in a source.
// The only required field is the name.
// Min and Max times are used to read
// sources in an order that resolves
// windows efficiently, as is the
// time order.
type Metadata struct {
	Name       string
	Addressing Addressing
	Size       int64
	MinTime    time.Time
	MaxTime    time.Time
	TimeOrder  TimeOrder
}

func (m Metadata) String() string {
	return fmt.Sprintf("name: %v, size: %v, min time: %v, max time: %v, time order: %v, addressing: %v",
		m.Name, m.Size, m.MinTime, m.MaxTime, m.TimeOrder, m.Addressing)
}

// Sources of data.
type Sources interface {
	Setup(graphType, graphName string, conf []byte) ([]Source, error)
}

// Source of data.
type Source interface {
	// Metadata about the source. Used for setting
	// up more efficient processing.
	Metadata() Metadata
	// Init the source. Init is called just before
	// a source is actively going to be used.
	Init() error
	// Stop the source and clean up. Stop is only
	// called if Init has been called.
	Stop() error
	// Take next item to consume. When a source
	// is done it should return io.EOF.
	Take(context.Context) (ID, interface{}, error)
}

// SortByMinTime the set of sources. The source's min
// time will be used for comparison.
func SortByMinTime(vss []Source) ([]Source, error) {
	// Range over all source, check their metadata.
	sorted := []*sortEntry{}
	for i, vs := range vss {
		// Append the min time, for sorting, and
		// the index of the actual source.
		sorted = append(sorted, &sortEntry{
			time:  vs.Metadata().MinTime,
			index: i,
		})
	}

	// Sort by the min time.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].time.Before(sorted[j].time)
	})

	// Return the result as just a slice of sources.
	all := []Source{}
	for _, s := range sorted {
		all = append(all, vss[s.index])
	}
	return all, nil
}

type sortEntry struct {
	time  time.Time
	index int
}
