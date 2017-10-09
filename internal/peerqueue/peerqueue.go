package peerqueue

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/lytics/grid"
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
	name                 string               // Name of peer.
	state                bool                 // Live or dead.
	optimisticState      bool                 // Live or dead.
	registered           map[string]bool      // Actors on this peer.
	optimisticRegistered map[string]time.Time // Possible actors on this peer.
}

func (pi *peerInfo) NumActors() int {
	return len(pi.registered) + len(pi.optimisticRegistered)
}

type selector struct {
	minAssigned string // Peer for min-assigned selection.
	maxAssigned string // Peer for max-assigned selection.
}

// New peer queue.
func New() *PeerQueue {
	return &PeerQueue{
		peers:                map[string]*peerInfo{},
		required:             map[string]*grid.ActorStart{},
		registered:           map[string]*peerInfo{},
		optimisticRegistered: map[string]*peerInfo{},
	}
}

type PeerQueue struct {
	mu                   sync.Mutex
	selector             selector
	peers                map[string]*peerInfo
	actorType            string
	required             map[string]*grid.ActorStart
	registered           map[string]*peerInfo
	optimisticRegistered map[string]*peerInfo
}

// IsRequired actor.
func (pq *PeerQueue) IsRequired(actor string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.required[actor]
	return ok
}

// Required set.
func (pq *PeerQueue) Required() []string {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	req := []string{}
	for actor := range pq.required {
		req = append(req, actor)
	}
	return req
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
func (pq *PeerQueue) Relocate() *RelocationPlan {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	l := 0
	for _, p := range pq.peers {
		if p.state == live {
			l++
		}
	}
	if l <= 0 {
		return nil
	}

	t := len(pq.required)
	a := int(math.Ceil(float64(t) / float64(l)))
	if a < 0 {
		a = 1
	}

	plan := NewRelocationPlan(pq.actorType, t, a)
	for name, p := range pq.peers {
		if p.state == dead {
			continue
		}
		burden := len(p.registered) - a
		plan.Peers = append(plan.Peers, name)
		plan.Count[name] = p.NumActors()
		plan.Burden[name] = burden
		if burden > 0 {
			for actor := range p.registered {
				def, ok := pq.required[actor]
				if !ok {
					continue
				}
				plan.Relocations = append(plan.Relocations, def)
				burden--
				if burden == 0 {
					break
				}
			}
		}
	}

	return plan
}

// MinAssigned returns the "next" living peer that
// has the least actors registered.
func (pq *PeerQueue) MinAssigned() (string, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.selector.minAssigned == "" {
		return "", ErrEmpty
	}
	return pq.selector.minAssigned, nil
}

// MaxAssigned returns the "next" living peer that
// has the most actors registered.
func (pq *PeerQueue) MaxAssigned() (string, error) {
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
			name:                 peer,
			registered:           map[string]bool{},
			optimisticRegistered: map[string]time.Time{},
		}
		pq.peers[peer] = pi
	}
	pi.state = live
	pi.optimisticState = live

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// OptimisticallyLive until an event marks the peer dead.
func (pq *PeerQueue) OptimisticallyLive(peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		pi = &peerInfo{
			name:                 peer,
			registered:           map[string]bool{},
			optimisticRegistered: map[string]time.Time{},
		}
		pq.peers[peer] = pi
	}
	pi.optimisticState = live

	// In the optimistic "live" case don't actually
	// recalculate the selector anything.
}

// Dead peer.
func (pq *PeerQueue) Dead(peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		pi = &peerInfo{
			name:                 peer,
			registered:           map[string]bool{},
			optimisticRegistered: map[string]time.Time{},
		}
		pq.peers[peer] = pi
	}
	pi.state = dead
	pi.optimisticState = dead

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// OptimisticallyDead until a real event marks the peer alive again.
func (pq *PeerQueue) OptimisticallyDead(peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		pi = &peerInfo{
			name:                 peer,
			registered:           map[string]bool{},
			optimisticRegistered: map[string]time.Time{},
		}
		pq.peers[peer] = pi
	}
	pi.optimisticState = dead

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// Peers currently live.
func (pq *PeerQueue) Peers() map[string]struct{} {
	peers := map[string]struct{}{}
	for peer, info := range pq.peers {
		if info.state != live {
			continue
		}
		peers[peer] = struct{}{}
	}
	return peers
}

// IsRegistered returns true if the actor has been
// registered.
func (pq *PeerQueue) IsRegistered(actor string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.registered[actor]
	return ok
}

// IsOptimisticallyRegistered returns true if the actor
// has been optimistically registered.
func (pq *PeerQueue) IsOptimisticallyRegistered(actor string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.optimisticRegistered[actor]
	return ok
}

// NumRegistered returns the current number of actors
// registered.
func (pq *PeerQueue) NumRegistered() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return len(pq.registered)
}

// NumOptimisticallyRegistered returns the current number
// of actors optimistically registered.
func (pq *PeerQueue) NumOptimisticallyRegistered() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return len(pq.optimisticRegistered)
}

// NumRegisteredOn the peer.
func (pq *PeerQueue) NumRegisteredOn(peer string) int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		return 0
	}
	return len(pi.registered)
}

// NumOptimisticallyRegisteredOn the peer.
func (pq *PeerQueue) NumOptimisticallyRegisteredOn(peer string) int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		return 0
	}
	return len(pi.optimisticRegistered)
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
	if pi, ok := pq.registered[actor]; ok {
		delete(pi.registered, actor)
		delete(pq.registered, actor)
	}
	// Check if the actor was optimistically assigned
	// to a peer. Since this method represents a REAL
	// registration, it overrides any optimistic
	// registration.
	if pi, ok := pq.optimisticRegistered[actor]; ok {
		delete(pi.optimisticRegistered, actor)
		delete(pq.optimisticRegistered, actor)
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

// OptimisticallyRegister an actor, ie: no confirmation has
// arrived that the actor is actually running the the peer,
// but it has been requested to run on the peer.
func (pq *PeerQueue) OptimisticallyRegister(actor, peer string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.peers[peer]
	if !ok {
		return
	}

	// CRITICAL
	// Check if this actor is already registered
	// under a different peer. This could happen
	// if optimistic-registered is called twice
	// without a unregister inbetween.
	if pi, ok := pq.optimisticRegistered[actor]; ok {
		delete(pi.optimisticRegistered, actor)
		delete(pq.optimisticRegistered, actor)
	}

	// Update the set of actors for
	// this peer.
	pi.optimisticRegistered[actor] = time.Now()

	// Update the global actor to peer
	// mapping.
	pq.optimisticRegistered[actor] = pi

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

// Unregister the actor from its current peer.
func (pq *PeerQueue) Unregister(actor string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Remove optimistic registrations, since
	// this is a REAL unregister, the actor
	// is for sure not running.
	pi, ok := pq.optimisticRegistered[actor]
	if ok {
		delete(pi.optimisticRegistered, actor)
		delete(pq.optimisticRegistered, actor)
	}

	// Remove registrations.
	pi, ok = pq.registered[actor]
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

// OptimisticallyUnregister the actor, ie: no confirmation has
// arrived that the actor is NOT running on the peer, but
// perhaps because of a failed request to the peer to start
// the actor it is known that likely the actor is not running.
func (pq *PeerQueue) OptimisticallyUnregister(actor string) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pi, ok := pq.optimisticRegistered[actor]
	if !ok {
		// Never registered.
		return
	}
	delete(pi.optimisticRegistered, actor)
	delete(pq.optimisticRegistered, actor)

	// Recalculate which peers are next
	// in the various selection schemes.
	pq.recalculateSelector()
}

func (pq *PeerQueue) recalculateSelector() {
	if len(pq.peers) == 0 {
		pq.selector = selector{}
		return
	}

	minC := math.MaxInt64 // Min count.
	minP := ""            // Peer with min count.
	maxC := math.MinInt64 // Max count.
	maxP := ""            // Peer with max count.
	for name, pi := range pq.peers {
		if pi.state == dead || pi.optimisticState == dead {
			continue
		}
		if minC > pi.NumActors() {
			minC = pi.NumActors()
			minP = name
		}
		if maxC < pi.NumActors() {
			maxC = pi.NumActors()
			maxP = name
		}
	}

	pq.selector.minAssigned = minP
	pq.selector.maxAssigned = maxP
}
