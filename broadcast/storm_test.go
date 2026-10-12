package broadcast_test

import (
	"math/rand"
	"testing"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

type stormPin struct {
	admission broadcast.Operation
	act       broadcast.Operation
}

type stormMark struct {
	evt.Mark
	Slot int64
}

type stormEffects struct {
	seen map[int64][]int64
}

func (s *stormEffects) Next(op broadcast.Operation) error {
	s.seen[op.Key()] = append(s.seen[op.Key()], op.Signal().(stormMark).Slot)
	return nil
}

func TestStormShuffledDeliveryConverges(t *testing.T) {
	const peers = 7
	const keys = 10
	const versions = 40

	workload := []stormPin{}
	tick := uint64(1)
	mint := func(member int64) broadcast.Mint {
		tick++
		return broadcast.Mint{Step: tick, Member: member}
	}
	for key := int64(1); key <= keys; key++ {
		holding := []broadcast.Mint{}
		for v := int64(0); v < versions; v++ {
			member := 1 + (key+v)%peers
			var act broadcast.Operation
			switch {
			case v%4 == 3:
				act = broadcast.NewOperation(mint(member), 50, key, 0, 0, stormMark{Slot: v})
			case len(holding) == 2:
				act = broadcast.NewOperation(mint(member), 50, key, 0, 0, broadcast.Release{Claim: holding[0]})
				holding = holding[1:]
			default:
				act = broadcast.NewOperation(mint(member), 50, key, 0, 0, broadcast.Claim{Talk: v})
				holding = append(holding, act.Mint())
			}
			admission := broadcast.NewOperation(mint(member), 50, key, v, v+1, broadcast.Admission{Act: act.Mint()})
			workload = append(workload, stormPin{admission: admission, act: act})
		}
	}

	folds := make([]broadcast.Fold, peers)
	effects := make([]*stormEffects, peers)
	for p := 0; p < peers; p++ {
		rng := rand.New(rand.NewSource(int64(p)*7919 + 13))
		delivery := append([]stormPin{}, workload...)
		rng.Shuffle(len(delivery), func(i, j int) { delivery[i], delivery[j] = delivery[j], delivery[i] })
		for _, i := range rng.Perm(len(delivery))[:len(delivery)/5] {
			delivery = append(delivery, delivery[i])
		}
		effects[p] = &stormEffects{seen: map[int64][]int64{}}
		folds[p] = broadcast.NewFold(func(int64) int { return 2 }, effects[p])
		arrived := map[broadcast.Mint]bool{}
		for _, d := range delivery {
			if !arrived[d.act.Mint()] {
				arrived[d.act.Mint()] = true
				folds[p].Post(d.act)
			}
			folds[p].Pin(d.admission, d.act)
		}
	}

	for p := 0; p < peers; p++ {
		if n := len(folds[p].Acts()); n != 0 {
			t.Fatalf("peer %d still carries %d unsettled acts", p, n)
		}
		for key := int64(1); key <= keys; key++ {
			if head := folds[p].Head(key); head != versions {
				t.Fatalf("peer %d key %d ended at version %d, want %d", p, key, head, versions)
			}
			want, got := folds[0].Checkpoint(key), folds[p].Checkpoint(key)
			if len(want.Holds) != len(got.Holds) {
				t.Fatalf("peer %d key %d holds %d, peer 0 holds %d", p, key, len(got.Holds), len(want.Holds))
			}
			for i := range want.Holds {
				if want.Holds[i] != got.Holds[i] {
					t.Fatalf("peer %d diverges on key %d at hold %d", p, key, i)
				}
			}
			if len(got.Holds) > 2 {
				t.Fatalf("peer %d key %d has %d holders over two places", p, key, len(got.Holds))
			}
			slots := effects[p].seen[key]
			if len(slots) != versions/4 {
				t.Fatalf("peer %d key %d ran %d effects, want %d, each once", p, key, len(slots), versions/4)
			}
			for i := 1; i < len(slots); i++ {
				if slots[i] <= slots[i-1] {
					t.Fatalf("peer %d key %d ran its effects out of the chain's order: %v", p, key, slots)
				}
			}
		}
	}
	t.Logf("storm: %d peers, %d pins shuffled with a fifth delivered twice; heads, holders and effects identical", peers, len(workload))
}

func TestACheckpointAheadOfTheFoldIsAdoptedAndWhatItCoversIsNotAppliedTwice(t *testing.T) {
	source := broadcast.NewFold(func(int64) int { return 2 }, nil)
	behind := broadcast.NewFold(func(int64) int { return 2 }, nil)
	pins := []stormPin{}
	for v := int64(0); v < 6; v++ {
		act := broadcast.NewOperation(broadcast.Mint{Step: uint64(10 + v), Member: 1 + v%3}, 50, 7, 0, 0, broadcast.Claim{})
		if v >= 2 {
			act = broadcast.NewOperation(broadcast.Mint{Step: uint64(10 + v), Member: 1 + v%3}, 50, 7, 0, 0, broadcast.Release{Claim: pins[v-2].act.Mint()})
		}
		pins = append(pins, stormPin{
			admission: broadcast.NewOperation(broadcast.Mint{Step: uint64(100 + v), Member: 1 + v%3}, 50, 7, v, v+1, broadcast.Admission{Act: act.Mint()}),
			act:       act,
		})
	}
	for _, p := range pins[:4] {
		source.Post(p.act)
		source.Pin(p.admission, p.act)
	}
	behind.Post(pins[0].act)
	behind.Pin(pins[0].admission, pins[0].act)
	behind.Post(pins[1].act)

	if behind.Post(pins[1].act) {
		t.Fatal("an unsettled act was taken twice")
	}

	if !behind.Adopt(source.Checkpoint(7)) {
		t.Fatal("a checkpoint ahead of the fold was not adopted")
	}
	if behind.Adopt(source.Checkpoint(7)) {
		t.Fatal("the same checkpoint was adopted twice")
	}
	behind.Pin(pins[1].admission, pins[1].act)
	if behind.Head(7) != 4 || len(behind.Holders(7)) != 0 || len(behind.Acts()) != 0 {
		t.Fatalf("head %d, %d holders, %d unsettled after adopting version 4", behind.Head(7), len(behind.Holders(7)), len(behind.Acts()))
	}
	for _, p := range pins[4:] {
		behind.Post(p.act)
		behind.Pin(p.admission, p.act)
	}
	if behind.Head(7) != 6 {
		t.Fatalf("the fold did not continue from the checkpoint: head %d", behind.Head(7))
	}
}
