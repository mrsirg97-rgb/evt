package broadcast_test

import (
	"context"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

func cycle(t *testing.T, s *seat, key int64, prio int) {
	t.Helper()
	got := make(chan broadcast.Mint, 1)
	s.claim.Claim(key, prio, 0, func(m broadcast.Mint, ok bool) {
		if ok {
			got <- m
		}
	})
	var m broadcast.Mint
	select {
	case m = <-got:
	case <-time.After(3 * time.Second):
		t.Fatalf("seat %d: claim never admitted", s.id)
	}
	s.claim.Release(m, prio)
	awaitWithin(t, "release", 3*time.Second, func() bool { return !holds(s, key, s.id) })
}

func TestAMemberWithHistoryLeavesWhileAnotherHolds(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3)
	defer stops(seats)
	a, b, c := seats[0], seats[1], seats[2]
	await(t, "room", func() bool { return seated(a) && seated(b) && seated(c) })

	cycle(t, a, 1, 30)
	await(t, "empty everywhere", func() bool { return holdersCount(b, 1) == 0 && holdersCount(c, 1) == 0 })

	b.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "B holds on all", func() bool { return holds(a, 1, 2) && holds(b, 1, 2) && holds(c, 1, 2) })

	a.closeAll()
	await(t, "A gone", func() bool { return !live(b, 1) && !live(c, 1) })

	entered := make(chan struct{}, 1)
	c.claim.Claim(1, 30, 0, func(_ broadcast.Mint, ok bool) {
		if ok {
			entered <- struct{}{}
		}
	})
	if holdersCount(b, 1) != 1 || holdersCount(c, 1) != 1 || !holds(b, 1, 2) || !holds(c, 1, 2) {
		t.Fatalf("a member that left took B's hold with it: B sees %d holders, C sees %d", holdersCount(b, 1), holdersCount(c, 1))
	}
	select {
	case <-entered:
		t.Fatalf("C was admitted to a key of one place while B never released")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAMemberWithHistoryLeavesThenOthersStillClaim(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3)
	defer stops(seats)
	a, b, c := seats[0], seats[1], seats[2]
	await(t, "room", func() bool { return seated(a) && seated(b) && seated(c) })
	cycle(t, a, 1, 30)
	cycle(t, b, 1, 30)
	a.closeAll()
	await(t, "A gone", func() bool { return !live(b, 1) && !live(c, 1) })
	cycle(t, c, 1, 30)
	cycle(t, b, 1, 30)
}

func TestAHolderDiesThenALateJoinerClaims(t *testing.T) {
	dir := shortTemp(t)
	codec := wireCodec(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2)
	defer stops(seats)
	a, b := seats[0], seats[1]
	await(t, "room", func() bool { return seated(a) && seated(b) })
	a.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "A holds", func() bool { return holds(b, 1, 1) })
	a.closeAll()
	a.stop()
	await(t, "A gone", func() bool { return !live(b, 1) })

	third := newSeat(t, dir, 3, func(int64) int { return 1 }, codec)
	defer third.stop()
	dialRoom(t, dir, third, codec)
	await(t, "joined", func() bool { return seated(third) && live(b, 3) })
	await(t, "the dead holder released for on both", func() bool {
		return holdersCount(b, 1) == 0 && holdersCount(third, 1) == 0
	})
	cycle(t, third, 1, 30)
	cycle(t, b, 1, 30)
}

func TestTheCostOfARequestDoesNotGrowWithTheRoomsAge(t *testing.T) {
	engine := evt.NewEngine()
	loop := evt.NewEngine()
	fold := broadcast.NewFold(func(int64) int { return 2 }, nil)
	cl := broadcast.NewClaimant(engine, loop, 1, broadcast.NewMinter(1, broadcast.MachineClock()), fold, 30, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.Start(ctx)
	go loop.Start(ctx)
	cl.Settle()
	round := func(n int) time.Duration {
		start := time.Now()
		for i := 0; i < n; i++ {
			got := make(chan broadcast.Mint, 1)
			cl.Claim(1, 30, 0, func(m broadcast.Mint, ok bool) { got <- m })
			m := <-got
			cl.Release(m, 30)
		}
		return time.Since(start)
	}
	young := round(500)
	round(4000)
	old := round(500)
	tail := make(chan int, 1)
	engine.Add(evt.Func(func(context.Context) { tail <- len(fold.Acts()) }), 0)
	if n := <-tail; n > 1 {
		t.Fatalf("after 5,000 requests the fold still carries %d unsettled acts", n)
	}
	t.Logf("requests 1-500: %v each; requests 4,501-5,000: %v each", young/500, old/500)
	if old > 5*young+50*time.Millisecond {
		t.Fatalf("a request after 4,500 others costs %v, the first 500 cost %v", old/500, young/500)
	}
}

func TestClaimsAtOnePriorityEnterInTheOrderTheyWereMinted(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3, 4, 5)
	defer stops(seats)
	await(t, "room", func() bool {
		for _, s := range seats {
			if !seated(s) {
				return false
			}
		}
		return true
	})

	order := make(chan broadcast.Mint, 8)
	enter := func(s *seat) {
		s.claim.Claim(1, 30, 0, func(m broadcast.Mint, ok bool) {
			if ok {
				order <- m
			}
		})
	}
	next := func(want int64) broadcast.Mint {
		t.Helper()
		select {
		case m := <-order:
			if m.Member != want {
				t.Fatalf("member %d entered, member %d was next in the order", m.Member, want)
			}
			return m
		case <-time.After(3 * time.Second):
			t.Fatalf("member %d never entered", want)
		}
		return broadcast.Mint{}
	}

	enter(seats[0])
	held := next(1)
	for _, s := range seats[1:4] {
		enter(s)
		await(t, "the claim waiting on every member", func() bool {
			for _, other := range seats {
				if !waitingClaim(other, 1, s.id) {
					return false
				}
			}
			return true
		})
	}
	seats[0].claim.Release(held, 30)
	held = next(2)
	enter(seats[4])
	await(t, "the late claim waiting", func() bool { return waitingClaim(seats[0], 1, 5) })
	seats[1].claim.Release(held, 30)
	held = next(3)
	seats[2].claim.Release(held, 30)
	held = next(4)
	seats[3].claim.Release(held, 30)
	next(5)
}

func TestAHolderThatDiesIsReleasedForAndALateJoinerSeesTheSameKey(t *testing.T) {
	dir := shortTemp(t)
	codec := wireCodec(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3)
	defer stops(seats)
	a, b, c := seats[0], seats[1], seats[2]
	await(t, "room", func() bool { return seated(a) && seated(b) && seated(c) })

	a.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "A holds", func() bool { return holds(b, 1, 1) && holds(c, 1, 1) })
	waiting := make(chan struct{}, 1)
	b.claim.Claim(1, 30, 0, func(_ broadcast.Mint, ok bool) {
		if ok {
			waiting <- struct{}{}
		}
	})
	await(t, "B waits", func() bool { return waitingClaim(c, 1, 2) })

	a.closeAll()
	a.stop()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("the waiter never entered after the holder died")
	}
	await(t, "one holder, B, on the survivors", func() bool {
		return holdersCount(b, 1) == 1 && holdersCount(c, 1) == 1 && holds(b, 1, 2) && holds(c, 1, 2)
	})

	fourth := newSeat(t, dir, 4, func(int64) int { return 1 }, codec)
	defer fourth.stop()
	dialRoom(t, dir, fourth, codec)
	await(t, "the joiner agrees", func() bool {
		return seated(fourth) && holdersCount(fourth, 1) == 1 && holds(fourth, 1, 2)
	})
}
