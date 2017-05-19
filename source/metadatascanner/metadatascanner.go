package metadatascanner

import (
	"context"
	"io"
	"time"

	"github.com/lytics/flo/source"
)

// Scan by reading the source fully, useful for small data sets,
// in memory data sets, or test data sets. It uses the inspect
// function to find the timestamp of each event, and calculate
// the Metadata. If the source is not finite the call will never
// finish.
func Scan(vs source.Source, inspect func(interface{}) (time.Time, error)) (*source.Metadata, error) {
	var min time.Time
	var max time.Time
	var prv time.Time
	cnt := int64(0)
	o2n := true
	n2o := true
	for {
		timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, v, err := vs.Next(timeout)
		cancel()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		cnt++

		ts, err := inspect(v)
		if err != nil {
			return nil, err
		}
		if min.IsZero() {
			min = ts
		}
		if max.IsZero() {
			max = ts
		}
		if min.After(ts) {
			min = ts
		}
		if max.Before(ts) {
			max = ts
		}

		if cnt > 1 {
			if prv.After(ts) {
				o2n = false
			}
			if prv.Before(ts) {
				n2o = false
			}
		}
		prv = ts
	}
	var order source.TimeOrder
	if o2n {
		order = source.Ascending
	}
	if n2o {
		order = source.Descending
	}
	if o2n && n2o {
		order = source.Equal
	}

	return &source.Metadata{
		Size:      cnt,
		MinTime:   min,
		MaxTime:   max,
		TimeOrder: order,
	}, nil
}
