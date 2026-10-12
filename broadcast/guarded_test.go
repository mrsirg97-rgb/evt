package broadcast_test

import (
	"testing"

	"github.com/mrsirg97-rgb/evt/broadcast"
)

func write(step uint64, key, parent, next int64) broadcast.Operation {
	return broadcast.NewOperation(broadcast.Mint{Step: step, Member: 1, Nonce: 0}, 50, key, parent, next, broadcast.Claim{})
}

func TestGuardedMutualExclusion(t *testing.T) {
	state := broadcast.NewFold(nil, nil)
	seq := broadcast.NewGuardedSequencer(1 << 62)
	if err := seq.Apply(state, write(1, 5, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := seq.Apply(state, write(5, 5, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := seq.Apply(state, write(6, 5, 1, 2)); err != nil {
		t.Fatal(err)
	}
	order := seq.Order()
	if len(order) != 2 {
		t.Fatalf("want 2 admitted (genesis + winner), got %d", len(order))
	}
	if order[1].Mint().Step != 5 {
		t.Errorf("winner should be the earlier-tick write (5), got step %d", order[1].Mint().Step)
	}
}

func TestGuardedRetroactiveEviction(t *testing.T) {
	state := broadcast.NewFold(nil, nil)
	seq := broadcast.NewGuardedSequencer(1 << 62)
	if err := seq.Apply(state, write(1, 9, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := seq.Apply(state, write(6, 9, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if seq.Order()[1].Mint().Step != 6 {
		t.Fatal("B should provisionally hold the slot before A arrives")
	}
	if err := seq.Apply(state, write(5, 9, 1, 2)); err != nil {
		t.Fatal(err)
	}
	order := seq.Order()
	if len(order) != 2 {
		t.Fatalf("want 2 admitted after eviction, got %d", len(order))
	}
	if order[1].Mint().Step != 5 {
		t.Errorf("earlier-tick A (5) must retroactively win, got step %d", order[1].Mint().Step)
	}
	if seq.Reorgs() == 0 {
		t.Error("late earlier-tick arrival must register a reorg")
	}
}
