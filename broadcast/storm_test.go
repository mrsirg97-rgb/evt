package broadcast_test

import (
	"math/rand"
	"testing"

	"github.com/mrsirg97-rgb/evt/broadcast"
)

type stormState struct {
	heads map[int64]int64
}

func (s *stormState) Next(op broadcast.Operation) error {
	s.heads[op.Key()] = op.Version()
	return nil
}

func (s *stormState) Previous(op broadcast.Operation) error {
	return nil
}

func stormWrite(key int64, version int64, tick uint64) broadcast.Operation {
	return broadcast.NewOperation(broadcast.Mint{Step: tick, Member: 1, Nonce: 0}, 50, key, version, version+1, broadcast.Claim{})
}

func TestStormShuffledDeliveryConverges(t *testing.T) {
	const peers = 7
	const keys = 10
	const versions = 20

	workload := []broadcast.Operation{}
	tick := uint64(1)
	for key := int64(1); key <= keys; key++ {
		for v := int64(0); v < versions; v++ {
			workload = append(workload, stormWrite(key, v, tick), stormWrite(key, v, tick+1))
			tick += 2
		}
	}

	orders := make([][]broadcast.Operation, peers)
	states := make([]*stormState, peers)
	sequencers := make([]broadcast.GuardedSequencer, peers)

	for p := 0; p < peers; p++ {
		rng := rand.New(rand.NewSource(int64(p)*7919 + 13))
		delivery := append([]broadcast.Operation{}, workload...)
		rng.Shuffle(len(delivery), func(i, j int) {
			delivery[i], delivery[j] = delivery[j], delivery[i]
		})
		for _, i := range rng.Perm(len(delivery))[:len(delivery)/5] {
			delivery = append(delivery, delivery[i])
		}
		st := &stormState{heads: map[int64]int64{}}
		seq := broadcast.NewGuardedSequencer(uint64(len(workload)) * 4)
		for _, op := range delivery {
			if err := seq.Apply(st, op); err != nil {
				t.Fatalf("peer %d apply: %v", p, err)
			}
		}
		orders[p] = seq.Order()
		states[p] = st
		sequencers[p] = seq
	}

	reference := orders[0]
	if len(reference) == 0 {
		t.Fatal("the storm must admit operations")
	}
	for p := 1; p < peers; p++ {
		if len(orders[p]) != len(reference) {
			t.Fatalf("peer %d admitted %d, peer 0 admitted %d", p, len(orders[p]), len(reference))
		}
		for i := range reference {
			if orders[p][i].Mint() != reference[i].Mint() || orders[p][i].Key() != reference[i].Key() {
				t.Fatalf("peer %d diverges at %d", p, i)
			}
		}
	}
	for p := 1; p < peers; p++ {
		if len(states[p].heads) != len(states[0].heads) {
			t.Fatalf("peer %d state size %d vs %d", p, len(states[p].heads), len(states[0].heads))
		}
		for key, head := range states[0].heads {
			if states[p].heads[key] != head {
				t.Fatalf("peer %d state diverges on key %d: %d vs %d", p, key, states[p].heads[key], head)
			}
		}
	}
	for key := int64(1); key <= keys; key++ {
		if states[0].heads[key] != versions {
			t.Fatalf("key %d ended at version %d, want %d", key, states[0].heads[key], versions)
		}
	}
	if int64(len(reference)) != keys*versions {
		t.Fatalf("admitted %d, want exactly %d (one per version slot)", len(reference), keys*versions)
	}
	t.Logf("storm: %d peers, %d operations (+dupes), admitted %d, reorgs %d, refused %d — zero divergence",
		peers, len(workload), len(reference), sequencers[0].Reorgs(), sequencers[0].Refused())
}

func TestStormWindowRefusesStaleness(t *testing.T) {
	st := &stormState{heads: map[int64]int64{}}
	seq := broadcast.NewGuardedSequencer(4)

	for v := int64(0); v < 10; v++ {
		if err := seq.Apply(st, stormWrite(1, v, uint64(v+1))); err != nil {
			t.Fatal(err)
		}
	}
	admitted := len(seq.Order())

	stale := stormWrite(2, 0, 1)
	if err := seq.Apply(st, stale); err != nil {
		t.Fatal(err)
	}
	if seq.Refused() != 1 {
		t.Fatalf("staleness must be refused, refused=%d", seq.Refused())
	}
	if len(seq.Order()) != admitted {
		t.Fatal("a refused operation must not join the order")
	}
	if _, ok := st.heads[2]; ok {
		t.Fatal("a refused operation must not touch state")
	}
}
