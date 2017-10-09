package schedule

import "errors"
import "sort"
import "hash/fnv"
import "fmt"

var (
	// ErrEmptyTerm when a term of zero peers tries to schedule.
	ErrEmptyTerm = errors.New("schedule: empty term")
)

const ringSize = 64

// New ring based on term.
func New(term []string) (*Ring, error) {
	if len(term) == 0 {
		return nil, ErrEmptyTerm
	}

	uniq := map[string]struct{}{}
	for _, peer := range term {
		uniq[peer] = struct{}{}
	}

	sorted := []string{}
	for peer := range uniq {
		sorted = append(sorted, peer)
	}
	sort.Strings(term)

	r := &Ring{
		ranges: make(map[uint64]string, ringSize),
	}
	for i := uint64(0); i < ringSize; i++ {
		r.ranges[i] = sorted[int(i)%len(sorted)]
	}

	return r, nil
}

// Ring formed by assignment of partitions
// to workers.
type Ring struct {
	ranges map[uint64]string
}

// Reducer of the given key in a specific graph.
func (r *Ring) Reducer(key, graphType, graphName string) string {
	h := fnv.New64()
	h.Write([]byte(key))
	i := h.Sum64()
	p := r.ranges[i]
	return fmt.Sprintf("worker-%v-%v-%v", p, graphType, graphName)
}
