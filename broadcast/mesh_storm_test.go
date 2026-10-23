package broadcast

import (
	"math/rand"
	"testing"
)

func TestMeshStormShuffledDeliveryConverges(t *testing.T) {
	const peers = 7
	const keys = 10
	const claims = 20

	acts := []Act{}
	tick := int64(1)
	for key := int64(1); key <= keys; key++ {
		for nonce := int64(0); nonce < claims; nonce++ {
			acts = append(acts, testAct(tick, 1, nonce, key, 20, "b", ActClaim))
			tick++
		}
	}

	orders := make([][]Act, peers)
	states := make([]*meshState, peers)
	for p := 0; p < peers; p++ {
		rng := rand.New(rand.NewSource(int64(p)*7919 + 13))
		delivery := append([]Act{}, acts...)
		rng.Shuffle(len(delivery), func(i, j int) {
			delivery[i], delivery[j] = delivery[j], delivery[i]
		})
		for _, i := range rng.Perm(len(delivery))[:len(delivery)/5] {
			delivery = append(delivery, delivery[i])
		}
		state := NewMeshState().(*meshState)
		seq := NewSequencer(int64(len(acts)) * 4)
		for _, act := range delivery {
			if err := seq.Apply(state, act); err != nil {
				t.Fatalf("peer %d apply: %v", p, err)
			}
		}
		orders[p] = seq.Order()
		states[p] = state
	}

	reference := orders[0]
	if len(reference) == 0 {
		t.Fatal("the storm must admit acts")
	}
	for p := 1; p < peers; p++ {
		if len(orders[p]) != len(reference) {
			t.Fatalf("peer %d admitted %d, peer 0 admitted %d", p, len(orders[p]), len(reference))
		}
		for i := range reference {
			if orders[p][i].Mint() != reference[i].Mint() {
				t.Fatalf("peer %d diverges at %d", p, i)
			}
		}
	}
	for p := 1; p < peers; p++ {
		if len(states[p].holds) != len(states[0].holds) {
			t.Fatalf("peer %d holds %d machines, peer 0 holds %d", p, len(states[p].holds), len(states[0].holds))
		}
		for machine, m := range states[0].holds {
			other, ok := states[p].holds[machine]
			if !ok || len(other) != len(m) {
				t.Fatalf("peer %d diverges on machine %s", p, machine)
			}
			for mint, act := range m {
				if other[mint].Mint() != act.Mint() {
					t.Fatalf("peer %d diverges on machine %s mint %v", p, machine, mint)
				}
			}
		}
	}
}
