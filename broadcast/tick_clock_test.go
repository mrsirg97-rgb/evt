package broadcast

import (
	"sync"
	"testing"
	"time"
)

const tickTestPeriod = 50 * time.Millisecond

func tickTestClock() MeshClock {
	return NewTickClock(tickTestPeriod, 4, 30)
}

func TestTickClockCountsTheMachinesTimeInPeriods(t *testing.T) {
	a, b := NewTickClock(tickTestPeriod, 4, 30), NewTickClock(tickTestPeriod, 4, 30)
	first := a.Step()
	if first == 0 {
		t.Fatal("the machine's tick is never zero")
	}
	if apart := int64(b.Step()) - int64(first); apart < 0 || apart > 1 {
		t.Fatalf("two clocks on one machine read the same tick, apart by %d", apart)
	}
	time.Sleep(3 * tickTestPeriod)
	if passed := a.Step() - first; passed < 2 || passed > 4 {
		t.Fatalf("three periods pass as three ticks, passed %d", passed)
	}
}

func TestTickClockRefusesATickAheadOfTheWindow(t *testing.T) {
	clock := NewTickClock(tickTestPeriod, 4, 30)
	now := clock.Step()
	if !clock.Record(now, nil, nil) || !clock.Record(now+2, nil, nil) || !clock.Record(now-100, nil, nil) {
		t.Fatal("a tick at, just ahead of, or behind this machine's is recorded: staleness is the sequencer's")
	}
	if clock.Record(now+50, nil, nil) {
		t.Fatal("a tick further ahead than the window must be refused, or one wrong clock moves every horizon")
	}
	if clock.Record(0, nil, nil) {
		t.Fatal("tick zero is no tick")
	}
	if clock.Trust(now+50, nil, nil) {
		t.Fatal("the machine's clock trusts nothing")
	}
}

func TestMeshOnTheTickClockConverges(t *testing.T) {
	const rounds = 4
	meshes := meshRoomOn(t, tickTestClock, 3, nil, fullMesh(3))
	startAll(t, meshes)

	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := range meshes {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, ok := meshes[i].Post(int64(100+i), 20, Mint{}); !ok {
					t.Errorf("member %d must be alive to post", i)
				}
			}(i)
		}
		wg.Wait()
	}
	await(t, "the mesh must converge on one order", func() bool { return converged(t, meshes) })
	for key := int64(100); key < 103; key++ {
		if held := len(meshes[0].Holders(key)); held != rounds {
			t.Fatalf("key %d holds %d claims, want %d", key, held, rounds)
		}
	}
}

func TestMeshOnTheTickClockTellsPastTheHorizon(t *testing.T) {
	fired := make(chan Act, 8)
	meshes := meshRoomOn(t, tickTestClock, 2, func(i int, act Act) {
		if i == 1 {
			fired <- act
		}
	}, fullMesh(2))
	startAll(t, meshes)

	posted := time.Now()
	claim, ok := meshes[0].Post(7, 20, Mint{})
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	select {
	case act := <-fired:
		if act.Mint() != claim.Mint() {
			t.Fatalf("the told carried mint %v, want %v", act.Mint(), claim.Mint())
		}
		if waited := time.Since(posted); waited < 4*tickTestPeriod {
			t.Fatalf("an act is told once its tick is past the window, told after %v", waited)
		}
	case <-time.After(meshTestPatience):
		t.Fatal("the member's told never fired past the horizon")
	}
}

func TestMeshOnTheTickClockReleasesASilentDoorsHolds(t *testing.T) {
	meshes := meshRoomOn(t, tickTestClock, 3, nil, fullMesh(3))
	startAll(t, meshes)

	if _, ok := meshes[0].Post(7, 20, Mint{}); !ok {
		t.Fatal("the member must be alive to post")
	}
	await(t, "the claim must cross before the silence", func() bool {
		return len(meshes[1].Holders(7)) == 1 && len(meshes[2].Holders(7)) == 1
	})
	meshes[0].Stop()
	await(t, "a silent door's holds must be released", func() bool {
		return len(meshes[1].Holders(7)) == 0 && len(meshes[2].Holders(7)) == 0
	})
}

func TestMeshOnTheTickClockTakesALateJoiner(t *testing.T) {
	meshes := meshRoomOn(t, tickTestClock, 3, nil, fullMesh(3))
	ctx := t.Context()
	t.Cleanup(func() {
		for i := range meshes {
			meshes[i].Stop()
		}
	})
	go meshes[0].Start(ctx)
	go meshes[1].Start(ctx)
	awaitTicking(t, meshes[:2])
	time.Sleep(10 * tickTestPeriod)
	go meshes[2].Start(ctx)
	awaitTicking(t, meshes[2:])

	mine, ok := meshes[2].Post(7, 20, Mint{})
	if !ok {
		t.Fatal("a joiner on the machine's clock posts at once")
	}
	theirs, ok := meshes[0].Post(8, 20, Mint{})
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	await(t, "a claim must cross both ways between the joiner and the mesh", func() bool {
		a, b := meshes[0].Holders(7), meshes[2].Holders(8)
		return len(a) == 1 && a[0].Mint() == mine.Mint() && len(b) == 1 && b[0].Mint() == theirs.Mint()
	})
}

func TestAMemberRefusesItsOwnIdFromAnotherMachine(t *testing.T) {
	m := meshRoomOn(t, tickTestClock, 1, nil, fullMesh(1))[0].(*mesh)
	t.Cleanup(m.Stop)
	own := NewAct(m.minter.Mint(), 7, 20, m.Machine(), ActClaim, nil, m.Clock().Output(), m.Clock().Proof())
	if !m.absorb(own) {
		t.Fatal("the member folds its own claim")
	}
	other := NewAct(Mint{Step: m.Clock().Step(), Member: m.id, Nonce: 99}, 8, 20, "machine-z", ActClaim, nil, nil, nil)
	if m.absorb(other) {
		t.Fatal("an act under this member's id from another machine is refused")
	}
	if len(m.state.holders(8)) != 0 || m.seen["machine-z"] {
		t.Fatal("a refused act leaves no hold and no machine")
	}
	m.learn("peer", NewMessage(m.id, true))
	for range m.cfg.Rounds() {
		if id, ok := m.addrID["peer"]; ok {
			m.expire(id)
		}
	}
	if !m.seen[m.cfg.Machine()] || len(m.state.holders(7)) != 1 {
		t.Fatal("a silent peer under this member's id never expires the member itself")
	}
}
