package source

import (
	"context"
	"fmt"
	"sort"
	"sync"
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

// NewItem from a source of data. Argument v is the value of the item.
// Argument c is the checkpoint associated with the item if the item
// is from a source that uses checkpoints. Argument done is a function
// used for ack/nack and will be called after the item is processed.
// Both c and done are optional, and different source types will use
// them differently.
func NewItem(v interface{}, c interface{}, done func(bool)) *Item {
	return &Item{
		v:    v,
		c:    c,
		done: done,
	}
}

// Item from a source of data.
type Item struct {
	v    interface{}
	c    interface{}
	done func(bool)
	once sync.Once
}

// Done true is called when the message has been
// processed, but possibly not sunk. False is
// passed if the message fails to process.
func (a *Item) Done(flg bool) {
	if a.done != nil {
		a.once.Do(func() { a.done(flg) })
	}
}

// Value of item.
func (a *Item) Value() interface{} {
	return a.v
}

// Checkpoint associated with item, if any.
func (a *Item) Checkpoint() interface{} {
	return a.c
}

func (a *Item) String() string {
	return fmt.Sprintf("value: %v, checkpoint: %v", a.v, a.c)
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
	// a source is actively going to be used. If
	// checkpoint is non-nil, and the source uses
	// checkpoints, the source will initialize its
	// state from the checkpoint.
	Init(checkpoint interface{}) error
	// Stop the source and clean up. Stop is only
	// called if Init has been called.
	Stop() error
	// Take next item to consume. When a source
	// is done it should return io.EOF.
	Take(ctx context.Context) (*Item, error)
}

// SortByMinTime the set of sources. The source's min
// time will be used for comparison.
func SortByMinTime(vss []Source) ([]Source, error) {
	// Clone vss.
	sorted := make([]Source, len(vss))
	copy(sorted, vss)

	// Sort by the min time.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Metadata().MinTime.Before(sorted[j].Metadata().MinTime)
	})

	// Return the result as just a slice of sources.
	return sorted, nil
}

type sortEntry struct {
	time  time.Time
	index int
}
