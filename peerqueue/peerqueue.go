package peerqueue

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/lytics/grid/grid.v3"
)

const (
	live = true
	dead = false
)

var (
	ErrEmpty             = errors.New("empty")
	ErrActorTypeMismatch = errors.New("actor type mismatch")
)

type peerInfo struct {
	name       string          // Name of peer.
	state      bool            // Live or dead.
	registered map[string]bool // Actors on this peer.
}

type selector struct {
	live        []string // Live peers.
	roundRobin  uint64   // Index for round-robin selection.
	minAssigned string   // Peer for min-assigned selection.
	maxAssigned string   // Peer for max-assigned selection.
}

// New peer queue.
func New() *PeerQueue {
	return &PeerQueue{
		peers:      map[string]*peerInfo{},
		required:   map[string]*grid.ActorStart{},
		registered: map[string]*peerInfo{},
	}
}

type PeerQueue struct {
	mu         sync.Mutex
	selector   selector
	peers      map[string]*peerInfo
	actorType  string
	required   map[string]*grid.ActorStart
	registered map[string]*peerInfo
}

// IsRequired actor.
func (pq *PeerQueue) IsRequired(actor string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.required[actor]
	return ok
}

// ActorType of this peer queue.
func (pq *PeerQueue) ActorType() string {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.actorType
}

// SetRequired flag on actor. If it's type does not match
// the type of previously set actors an error is returned.
func (pq *PeerQueue) SetRequired(def *grid.ActorStart) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.actorType == "" {
		pq.actorType = def.Type
	}
	if pq.actorType != def.Type {
		return ErrActorTypeMismatch
	}
	pq.required[def.Name] = def
	return nil
}

// UnsetRequired flag on actor.
func (pq *PeerQueue) UnsetRequired(actor string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	delete(pq.required, actor)
	if len(pq.required) == 0 {
		pq.actorType = ""
	}
}

// Missing actors that are required but not registered.
func (pq *PeerQueue) Missing() []*grid.ActorStart {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	var missing []*grid.ActorStart
	for name, def := range pq.required {
		if _, ok := pq.registered[name]; !ok {
			missing = append(missing, def)
		}
	}
	return missing
}

// Relocate actors from peers that have an unfair number of
// actors, where unfair is defined as the integer ceiling
// of the average.
func (pq *PeerQueue) Relocate() []*grid.ActorStart {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	l := len(pq.peers)
	if l < 0 {
		return nil
	}

	r := len(pq.required)
	a := int(math.Ceil(float64(r) / float64(l)))
	if a < 0 {
		a = 1
	}

	relocate := []*grid.ActorStart{}
	for _, p := range pq.peers {
		burden := len(p.registered) - a
		if burden > 0 {
			for actor := range p.registered {
				def, ok := pq.required[actor]
				if !ok {
					continue
				}
				relocate = append(relocate, def)
				burden--
				if burden == 0 {
					break
				}
			}
		}
	}

	return relocate
}

// RoundRobin returns the "next" living peer in a round
// robin fashion.
func (pq *PeerQueue) RoundRobin() (string, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.selector.live) == 0 {
		return "", ErrEmpty
	}

	next := int(pq.selector.roundRobin % uint64(len(pq.selector.live)))
	peer := pq.selector.live[next]
	pq.selector.roundRobin++
	return peer, nil
}

// MinAssigned returns the "next" living peer that
// has the least actors registered.
func (pq PeerQueue) MinAssigned() (string, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.selector.minAssigned == "" {
		return "", ErrEmpty
	}
	return pq.selector.minAssigned, nil
}

// MaxAssigned returns the "next" living peer that
// has the most actors registered.
func (pq PeerQueue) MaxAssigned() (string, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.selector.maxAssigned == "" {
		return "", ErrEmpty
	}
	return pq.selector.maxAssigned, nil
}

// Live peer.
func (pq *PeerQueue) Live(peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		pi = &peerInfo{
			name:       peer,
			registered: map[string]bool{},
		}
		pq.peers[peer] = pi
	}
	pi.state = live

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// Dead peer.
func (pq *PeerQueue) Dead(peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		pi = &peerInfo{
			name:       peer,
			registered: map[string]bool{},
		}
		pq.peers[peer] = pi
	}
	pi.state = dead

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// IsRegistered returns true if the actor has been
// registered.
func (pq *PeerQueue) IsRegistered(actor string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.registered[actor]
	return ok
}

// NumRegistered returns the current number of actors
// registered.
func (pq *PeerQueue) NumRegistered() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return len(pq.registered)
}

// Register the actor to the peer.
func (pq *PeerQueue) Register(actor, peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		return
	}

	// CRITICAL
	// Check if this actor is already registered
	// under a different peer. This could happen
	// if registered is called twice without a
	// unregister inbetween.
	if actorPi, ok := pq.registered[actor]; ok {
		delete(actorPi.registered, actor)
	}

	// Update the set of actors for
	// this peer.
	pi.registered[actor] = true

	// Update the global actor to peer
	// mapping.
	pq.registered[actor] = pi

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// Unregister the actor from its current peer.
func (pq *PeerQueue) Unregister(actor string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.registered[actor]
	if !ok {
		// Never registered.
		return
	}
	delete(pi.registered, actor)
	delete(pq.registered, actor)

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

func (pq *PeerQueue) recalculateSelector() {
	if len(pq.peers) == 0 {
		pq.selector = selector{}
		return
	}

	minC := 0
	minP := ""
	maxC := 0
	maxP := ""
	sorted := []string{}
	for name, pi := range pq.peers {
		if pi.state == live {
			sorted = append(sorted, name)
		}
		if minC > len(pi.registered) {
			minC = len(pi.registered)
			minP = name
		}
		if maxC < len(pi.registered) {
			maxC = len(pi.registered)
			maxP = name
		}
	}
	sort.Strings(sorted)

	pq.selector.live = sorted
	pq.selector.minAssigned = minP
	pq.selector.maxAssigned = maxP
}
