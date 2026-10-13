package broadcast

import "testing"

func testAct(tick, member, nonce, key int64, priority int, machine string, kind ActKind) Act {
	return Act{
		Mint:     Mint{Step: uint64(tick), Member: member, Nonce: nonce},
		Key:      key,
		Priority: priority,
		Machine:  machine,
		Kind:     kind,
	}
}

func TestSequencerReorgsLateArrival(t *testing.T) {
	state := NewMeshState()
	seq := NewSequencer(1 << 62)

	seq.Apply(state, testAct(30, 1, 1, 7, 20, "b", ActClaim))
	seq.Apply(state, testAct(10, 1, 2, 7, 20, "b", ActClaim))
	seq.Apply(state, testAct(20, 1, 3, 7, 20, "b", ActClaim))

	order := seq.Order()
	want := []uint64{10, 20, 30}
	for i, act := range order {
		if act.Tick() != want[i] {
			t.Fatalf("position %d has tick %d, want %d", i, act.Tick(), want[i])
		}
	}
	if seq.Reorgs() == 0 {
		t.Fatal("late arrivals must have triggered at least one reorg")
	}
}

func TestSequencerSameTickTiebreak(t *testing.T) {
	stateA, stateB := NewMeshState(), NewMeshState()
	seqA, seqB := NewSequencer(1<<62), NewSequencer(1<<62)

	seqA.Apply(stateA, testAct(5, 2, 1, 7, 20, "a", ActClaim))
	seqA.Apply(stateA, testAct(5, 1, 1, 7, 20, "b", ActClaim))
	seqB.Apply(stateB, testAct(5, 1, 1, 7, 20, "b", ActClaim))
	seqB.Apply(stateB, testAct(5, 2, 1, 7, 20, "a", ActClaim))

	a, b := seqA.Order(), seqB.Order()
	if a[0].Mint != b[0].Mint || a[1].Mint != b[1].Mint {
		t.Fatalf("peers disagreed on the same-tick order: %v %v", a, b)
	}
	if a[0].Mint.Member != 1 {
		t.Fatalf("the lower member sorts first, got %d", a[0].Mint.Member)
	}
}

func TestSequencerRefusesStale(t *testing.T) {
	state := NewMeshState()
	seq := NewSequencer(2)

	seq.Apply(state, testAct(100, 1, 1, 7, 20, "a", ActClaim))
	seq.Apply(state, testAct(101, 1, 2, 7, 20, "a", ActClaim))
	seq.Apply(state, testAct(50, 1, 3, 7, 20, "a", ActClaim))

	if seq.Refused() != 1 {
		t.Fatalf("staleness must be refused, refused=%d", seq.Refused())
	}
	if len(seq.Order()) != 2 {
		t.Fatalf("a refused act must not join the order, len=%d", len(seq.Order()))
	}
}
