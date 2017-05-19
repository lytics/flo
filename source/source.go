package source

import (
	"context"
	"time"

	"fmt"
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

const (
	// NoID when an identifier is unknown.
	NoID ID = ""
)

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

func (m *Metadata) String() string {
	return fmt.Sprintf("name: %v, size: %v, min time: %v, max time: %v, time order: %v, addressing: %v",
		m.Name, m.Size, m.MinTime, m.MaxTime, m.TimeOrder, m.Addressing)
}

// Source of data.
type Source interface {
	// Metadata about the source. Used for setting
	// up more efficient processing.
	Metadata() (*Metadata, error)
	// Init the source, where ID is the start location
	// when the source is addressable sequentially,
	// otherwise set to NoID.
	Init(ID) error
	// Next item in the source.
	Next(context.Context) (ID, interface{}, error)
	// Stop the source and clean up.
	Stop() error
}
