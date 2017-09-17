package peerqueue

import (
	"testing"

	"github.com/lytics/grid"
)

func TestPeerLiveDead(t *testing.T) {
	pq := New()

	peers := map[string]bool{
		"peer0": true,
		"peer1": true,
	}

	for p := range peers {
		pq.Live(p)
	}
	for p := range peers {
		if pq.peers[p].state != live {
			t.Fatal("expected live peer")
		}
	}

	for p := range peers {
		pq.Dead(p)
	}
	for p := range peers {
		if pq.peers[p].state != dead {
			t.Fatal("expected dead peer")
		}
	}
}

func TestRegisterUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Register("writer0", "peer0")

	if pq.NumRegistered() != 1 {
		t.Fatal("expected 1 registered")
	}
	if pq.NumRegisteredOn("peer0") != 1 {
		t.Fatal("expected 1 registered on peer")
	}
	if !pq.IsRegistered("writer0") {
		t.Fatal("expected registered")
	}

	pq.Unregister("writer0")
	if pq.NumRegistered() != 0 {
		t.Fatal("expected 0 registered")
	}
	if pq.NumRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered on peer")
	}
	if pq.IsRegistered("writer0") {
		t.Fatal("expected not-registered")
	}
}

func TestOptimisticallyRegisterUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.OptimisticallyRegister("writer0", "peer0")

	if pq.NumOptimisticallyRegistered() != 1 {
		t.Fatal("expected 1 optimistically registered")
	}
	if pq.NumOptimisticallyRegisteredOn("peer0") != 1 {
		t.Fatal("expected 1 registered on peer")
	}
	if !pq.IsOptimisticallyRegistered("writer0") {
		t.Fatal("expected registered")
	}

	pq.OptimisticallyUnregister("writer0")
	if pq.NumRegistered() != 0 {
		t.Fatal("expected 0 optimistically registered")
	}
	if pq.NumOptimisticallyRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered on peer")
	}
	if pq.IsOptimisticallyRegistered("writer0") {
		t.Fatal("expected not-registered")
	}
}

func TestRelocateOne(t *testing.T) {
	pq := New()

	pq.SetRequired(grid.NewActorStart("writer0"))
	pq.SetRequired(grid.NewActorStart("writer1"))

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer0")

	plan := pq.Relocate()
	if len(plan.Relocations) != 1 {
		t.Fatal("expected one relocation")
	}
}

func TestRelocateZero(t *testing.T) {
	pq := New()

	pq.SetRequired(grid.NewActorStart("writer0"))
	pq.SetRequired(grid.NewActorStart("writer1"))

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer1")

	plan := pq.Relocate()
	if len(plan.Relocations) != 0 {
		t.Fatal("expected zero relocations")
	}
}

func TestRelocateOddPeersEvenActors(t *testing.T) {
	pq := New()

	for _, w := range []string{
		"writer0",
		"writer1",
		"writer2",
		"writer3",
	} {
		def := grid.NewActorStart(w)
		def.Type = "writer"
		pq.SetRequired(def)
	}
	for _, p := range []string{
		"peer0",
		"peer1",
		"peer2",
	} {
		pq.Live(p)
	}

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer1")
	pq.Register("writer2", "peer2")
	pq.Register("writer3", "peer0")

	plan := pq.Relocate()
	if len(plan.Relocations) != 0 {
		t.Fatal("expected zero relocations")
	}
}

func TestRelocateEvenPeersOddActors(t *testing.T) {
	pq := New()

	for _, w := range []string{
		"writer0",
		"writer1",
		"writer2",
	} {
		def := grid.NewActorStart(w)
		def.Type = "writer"
		pq.SetRequired(def)
	}
	for _, p := range []string{
		"peer0",
		"peer1",
	} {
		pq.Live(p)
	}

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer1")
	pq.Register("writer2", "peer0")

	plan := pq.Relocate()
	if len(plan.Relocations) != 0 {
		t.Fatal("expected zero relocations")
	}
}

func TestRelocateWithMinAssigned(t *testing.T) {
	pq := New()

	for _, p := range []string{
		"peer0",
		"peer1",
		"peer2",
	} {
		pq.Live(p)
	}

	for _, w := range []string{
		"writer0",
		"writer1",
		"writer2",
		"writer3",
	} {
		def := grid.NewActorStart(w)
		def.Type = "writer"
		pq.SetRequired(def)

		p, err := pq.MinAssigned()
		if err != nil {
			t.Fatal(err)
		}

		pq.Register(w, p)
	}

	plan := pq.Relocate()
	if len(plan.Relocations) != 0 {
		t.Fatal("expected zero relocations")
	}

	var p1 string
	for w, pi := range pq.registered {
		if w == "writer1" {
			p1 = pi.name
		}
	}

	pq.Unregister("writer1")
	pq.Dead(p1)

	for _, def := range pq.Missing() {
		if def.Name != "writer1" {
			t.Fatal("expected writer to be missing")
		}
	}

	p, err := pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	pq.Register("writer1", p)

	plan = pq.Relocate()
	if len(plan.Relocations) != 0 {
		t.Fatal("expected zero relocations")
	}
}

func TestMinAssignedWithRegister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")

	p, err := pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer1" {
		t.Fatal("expected min assigned")
	}

	pq.Register("writer1", "peer1")
	pq.Register("writer2", "peer1")

	p, err = pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer0" {
		t.Fatal("expected min assigned")
	}
}

func TestMinAssignedWithUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer1")
	pq.Register("writer2", "peer1")

	p, err := pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer0" {
		t.Fatal("expected min assigned")
	}

	pq.Unregister("writer1")
	pq.Unregister("writer2")

	p, err = pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer1" {
		t.Fatal("expected min assigned")
	}
}

func TestMinAssignedWithOptimisticallyRegister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.OptimisticallyRegister("writer0", "peer0")

	p, err := pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer1" {
		t.Fatal("expected min assigned")
	}

	pq.OptimisticallyRegister("writer1", "peer1")
	pq.OptimisticallyRegister("writer2", "peer1")

	p, err = pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer0" {
		t.Fatal("expected min assigned")
	}
}

func TestMinAssignedWithOptimisticallyUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.OptimisticallyRegister("writer0", "peer0")
	pq.OptimisticallyRegister("writer1", "peer1")
	pq.OptimisticallyRegister("writer2", "peer1")

	p, err := pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer0" {
		t.Fatal("expected min assigned")
	}

	pq.OptimisticallyUnregister("writer1")
	pq.OptimisticallyUnregister("writer2")

	p, err = pq.MinAssigned()
	if err != nil {
		t.Fatal(err)
	}
	if p != "peer1" {
		t.Fatal("expected min assigned")
	}
}

func TestRegisterRemoveOptimisticallyRegistered(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.OptimisticallyRegister("writer0", "peer0")

	if 0 != pq.NumRegistered() {
		t.Fatal("expected 0 registered")
	}
	if 1 != pq.NumOptimisticallyRegistered() {
		t.Fatal("expected 1 optimistically registered")
	}

	pq.Register("writer0", "peer0")
	if 1 != pq.NumRegistered() {
		t.Fatal("expected 1 registered")
	}
	if 0 != pq.NumOptimisticallyRegistered() {
		t.Fatal("expected 0 optimistically registered")
	}
}

func TestMinMaxAssigned(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")
	pq.Register("writer1", "peer1")
	pq.Register("writer2", "peer1")

	min, _ := pq.MinAssigned()
	if min != "peer0" {
		t.Fatal("expected min peer")
	}

	max, _ := pq.MaxAssigned()
	if max != "peer1" {
		t.Fatal("expected max peer")
	}
}

func TestMinMaxAssignedEmpty(t *testing.T) {
	pq := New()

	min, err := pq.MinAssigned()
	if err != ErrEmpty {
		t.Fatal("expected error-empty")
	}
	if min != "" {
		t.Fatal("expected no min peer")
	}

	max, err := pq.MaxAssigned()
	if err != ErrEmpty {
		t.Fatal("expected error-empty")
	}
	if max != "" {
		t.Fatal("expected no max peer")
	}
}

func TestSetUnsetRequired(t *testing.T) {
	pq := New()

	pq.SetRequired(grid.NewActorStart("writer"))
	if !pq.IsRequired("writer") {
		t.Fatal("expected required")
	}
	pq.UnsetRequired("writer")
	if pq.IsRequired("writer") {
		t.Fatal("expected not-required")
	}
}

func TestNumRegisteredOn(t *testing.T) {
	pq := New()

	if pq.NumRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered")
	}

	pq.Live("peer0")
	pq.Register("writer0", "peer0")
	if pq.NumRegisteredOn("peer0") != 1 {
		t.Fatal("expected 1 registered")
	}
}

func TestNumOptimisticallyRegisteredOn(t *testing.T) {
	pq := New()

	if pq.NumOptimisticallyRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered")
	}

	pq.Live("peer0")
	pq.OptimisticallyRegister("writer0", "peer0")
	if pq.NumOptimisticallyRegisteredOn("peer0") != 1 {
		t.Fatal("expected 1 registered")
	}
}

func TestRegisterWithZeroPeers(t *testing.T) {
	pq := New()

	pq.Register("writer0", "peer0")
	if pq.NumRegistered() != 0 {
		t.Fatal("expected 0 registered")
	}
}

func TestOptimisticallyRegisterWithZeroPeers(t *testing.T) {
	pq := New()

	pq.OptimisticallyRegister("writer0", "peer0")
	if pq.NumOptimisticallyRegistered() != 0 {
		t.Fatal("expected 0 registered")
	}
}

func TestDeadPeerThatWasNeverLive(t *testing.T) {
	pq := New()

	pq.Dead("peer0")
	if pq.peers["peer0"].state != dead {
		t.Fatal("expected dead peer")
	}
}

func TestRegisterTwiceWithoutUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.Register("writer0", "peer0")
	pq.Register("writer0", "peer1")

	if pq.NumRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered on peer")
	}
	if pq.NumRegisteredOn("peer1") != 1 {
		t.Fatal("expected 1 registered on peer")
	}
}

func TestOptimisticallyRegisterTwiceWithoutUnregister(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.Live("peer1")

	pq.OptimisticallyRegister("writer0", "peer0")
	pq.OptimisticallyRegister("writer0", "peer1")

	if pq.NumOptimisticallyRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered on peer")
	}
	if pq.NumOptimisticallyRegisteredOn("peer1") != 1 {
		t.Fatal("expected 1 registered on peer")
	}
}

func TestUnregisterWithOptimisticallyRegistered(t *testing.T) {
	pq := New()

	pq.Live("peer0")
	pq.OptimisticallyRegister("writer0", "peer0")
	if pq.NumOptimisticallyRegisteredOn("peer0") != 1 {
		t.Fatal("expected 1 registered on peer")
	}
	pq.Unregister("writer0")
	if pq.NumOptimisticallyRegisteredOn("peer0") != 0 {
		t.Fatal("expected 0 registered on peer")
	}
}
