package broadcast_test

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

type seat struct {
	id       int64
	engine   evt.Engine
	loop     evt.Engine
	claim    broadcast.Claimant
	listener net.Listener
	mu       sync.Mutex
	trs      []broadcast.Transport
	stop     func()
}

func shortTemp(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "evt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func wireCodec(t *testing.T) broadcast.Encoder {
	t.Helper()
	reg, err := broadcast.Protocol()
	if err != nil {
		t.Fatal(err)
	}
	return broadcast.NewJSONEncoder(reg)
}

func newSeat(t *testing.T, dir string, id int64, places func(int64) int, codec broadcast.Encoder) *seat {
	t.Helper()
	engine := evt.NewEngine()
	loop := evt.NewEngine()
	fold := broadcast.NewFold(places, nil)
	minter := broadcast.NewMinter(id, broadcast.MachineClock())
	claimant := broadcast.NewClaimant(engine, loop, id, minter, fold, 30, nil)
	listener, err := broadcast.Listen(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go engine.Start(ctx)
	go loop.Start(ctx)
	s := &seat{id: id, engine: engine, loop: loop, claim: claimant, listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			tr := broadcast.NewPipeTransport(0, conn, codec)
			s.track(tr)
			claimant.Accept(tr)
		}
	}()
	s.stop = func() {
		cancel()
		engine.Stop()
		loop.Stop()
		listener.Close()
	}
	return s
}

func dialRoom(t *testing.T, dir string, s *seat, codec broadcast.Encoder) {
	t.Helper()
	members, err := broadcast.Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range members {
		if peer == s.id {
			continue
		}
		conn, err := broadcast.Dial(dir, peer)
		if err != nil {
			t.Fatal(err)
		}
		tr := broadcast.NewPipeTransport(peer, conn, codec)
		s.track(tr)
		s.claim.Subscribe(tr)
	}
}

func seatMembers(t *testing.T, dir string, places func(int64) int, ids ...int64) []*seat {
	t.Helper()
	codec := wireCodec(t)
	seats := make([]*seat, 0, len(ids))
	for _, id := range ids {
		s := newSeat(t, dir, id, places, codec)
		seats = append(seats, s)
	}
	for _, s := range seats {
		dialRoom(t, dir, s, codec)
	}
	return seats
}

func (s *seat) track(tr broadcast.Transport) {
	s.mu.Lock()
	s.trs = append(s.trs, tr)
	s.mu.Unlock()
}

func (s *seat) closeAll() {
	s.mu.Lock()
	trs := append([]broadcast.Transport{}, s.trs...)
	s.mu.Unlock()
	for _, tr := range trs {
		tr.Close()
	}
}

func stops(seats []*seat) {
	for _, s := range seats {
		s.stop()
	}
}

func probe(s *seat, f func(c broadcast.Claimant) bool) bool {
	hit := make(chan bool, 1)
	s.engine.Add(evt.Func(func(context.Context) { hit <- f(s.claim) }), 5)
	return <-hit
}

func holds(s *seat, key int64, member int64) bool {
	return probe(s, func(c broadcast.Claimant) bool {
		for _, op := range c.Holders(key) {
			if op.Origin() == member {
				return true
			}
		}
		return false
	})
}

func holdersCount(s *seat, key int64) int {
	hit := make(chan int, 1)
	s.engine.Add(evt.Func(func(context.Context) { hit <- len(s.claim.Holders(key)) }), 5)
	return <-hit
}

func waitingClaim(s *seat, key int64, member int64) bool {
	return probe(s, func(c broadcast.Claimant) bool {
		for _, op := range c.Waiting(key) {
			if op.Origin() == member {
				return true
			}
		}
		return false
	})
}

func live(s *seat, id int64) bool {
	return probe(s, func(c broadcast.Claimant) bool {
		for _, peer := range c.Live() {
			if peer == id {
				return true
			}
		}
		return false
	})
}

func seated(s *seat) bool {
	return probe(s, func(c broadcast.Claimant) bool { return c.Seated() })
}

func await(t *testing.T, what string, cond func() bool) {
	t.Helper()
	awaitWithin(t, what, 2*time.Second, cond)
}

func awaitWithin(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waited for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func holdersOf(s *seat, key int64) []broadcast.Mint {
	hit := make(chan []broadcast.Mint, 1)
	s.engine.Add(evt.Func(func(context.Context) {
		mints := []broadcast.Mint{}
		for _, op := range s.claim.Holders(key) {
			mints = append(mints, op.Mint())
		}
		hit <- mints
	}), 5)
	return <-hit
}

func sameHolders(a, b []broadcast.Mint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func agree(t *testing.T, seats []*seat, keys ...int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, key := range keys {
			want := holdersOf(seats[0], key)
			for _, s := range seats[1:] {
				if !sameHolders(holdersOf(s, key), want) {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for _, key := range keys {
				for _, s := range seats {
					t.Logf("seat %d key %d: %v", s.id, key, holdersOf(s, key))
				}
			}
			t.Fatal("the holders never agreed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSevenPeersClaimContendedKeysAndALateJoinerAgrees(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2, 3, 4, 5, 6, 7)
	defer stops(seats)

	priorities := []int{90, 70, 50, 30, 20, 10, 0}
	for _, s := range seats {
		for key := int64(1); key <= 7; key++ {
			s.claim.Claim(key, priorities[(int(s.id)+int(key))%len(priorities)], 0, func(broadcast.Mint, bool) {})
		}
	}
	agree(t, seats, 1, 2, 3, 4, 5, 6, 7)

	codec := wireCodec(t)
	eighth := newSeat(t, dir, 8, func(int64) int { return 2 }, codec)
	dialRoom(t, dir, eighth, codec)
	agree(t, append(seats, eighth), 1, 2, 3, 4, 5, 6, 7)
}

func TestTwoInstancesStartingAtTheSameInstantEndInOneRoom(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2)
	defer stops(seats)
	await(t, "one room", func() bool {
		return live(seats[0], 2) && live(seats[1], 1) &&
			seated(seats[0]) && seated(seats[1])
	})
}

func TestAClaimIsNeverActedOnBeforeItIsFinal(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2)
	defer stops(seats)
	a, b := seats[0], seats[1]

	block := make(chan struct{})
	b.engine.Add(evt.Func(func(context.Context) { <-block }), 91)

	a.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	time.Sleep(120 * time.Millisecond)
	if holdersCount(a, 1) != 0 {
		t.Fatalf("a claim alone put its member in the holder set: %d", holdersCount(a, 1))
	}

	close(block)
	await(t, "the admission", func() bool { return holds(a, 1, 1) })
	await(t, "the member agrees", func() bool { return holds(b, 1, 1) })
}

func TestTheLoopCanBlockWhileTheRoomAnswers(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2)
	defer stops(seats)
	a, b := seats[0], seats[1]

	block := make(chan struct{})
	b.loop.Add(evt.Func(func(context.Context) { <-block }), 91)

	done := make(chan broadcast.Mint, 1)
	a.claim.Claim(1, 90, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted {
			done <- mint
		}
	})
	<-done
	if !holds(a, 1, 1) {
		t.Fatal("the claim became final and admitted while the peer's loop was blocked")
	}
	close(block)
}

func TestPriorityOutranksArrival(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2)
	defer stops(seats)
	a, b := seats[0], seats[1]

	order := make(chan int64, 2)
	b.claim.Claim(1, 30, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted {
			order <- mint.Member
		}
	})
	a.claim.Claim(1, 90, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted {
			order <- mint.Member
		}
	})
	if first := <-order; first != 1 {
		t.Fatalf("the 90 claim entered first, got member %d", first)
	}
	if second := <-order; second != 2 {
		t.Fatalf("the 30 claim followed, got member %d", second)
	}
}

func TestHighClaimOnAFullKeyEntersBeyondThePlaces(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3)
	defer stops(seats)
	a, b, c := seats[0], seats[1], seats[2]

	release := make(chan broadcast.Mint, 1)
	a.claim.Claim(1, 30, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted {
			release <- mint
		}
	})
	claim := <-release
	await(t, "A's hold on every member", func() bool { return holds(a, 1, 1) && holds(b, 1, 1) && holds(c, 1, 1) })

	b.claim.Claim(1, 90, 0, func(broadcast.Mint, bool) {})
	await(t, "the entry beyond the places", func() bool {
		for _, s := range seats {
			if holdersCount(s, 1) != 2 || !holds(s, 1, 2) {
				return false
			}
		}
		return true
	})
	for _, s := range seats {
		if !holds(s, 1, 1) {
			t.Fatal("the entry evicted the lower-priority holder")
		}
	}

	c.claim.Claim(1, 90, 0, func(broadcast.Mint, bool) {})
	time.Sleep(120 * time.Millisecond)
	for _, s := range seats {
		if holdersCount(s, 1) != 2 || holds(s, 1, 3) {
			t.Fatalf("a second claim entered while the entry was taken: %d holders", holdersCount(s, 1))
		}
	}

	a.claim.Release(claim, 30)
	await(t, "the release admits the waiter into the place", func() bool {
		for _, s := range seats {
			if holdersCount(s, 1) != 2 || !holds(s, 1, 3) || holds(s, 1, 1) {
				return false
			}
		}
		return true
	})
}

func TestShuffledConflictingClaimsEndWithOneHolder(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2, 3)
	defer stops(seats)

	done := make(chan int64, 3)
	for _, s := range seats {
		s.claim.Claim(1, 50, 0, func(mint broadcast.Mint, admitted bool) {
			if admitted {
				done <- mint.Member
			}
		})
	}
	winner := <-done
	await(t, "one holder on every member", func() bool {
		for _, s := range seats {
			if holdersCount(s, 1) != 1 {
				return false
			}
		}
		return true
	})
	for _, s := range seats {
		if !holds(s, 1, winner) {
			t.Fatalf("member %d's fold disagrees: the holder is not member %d", s.id, winner)
		}
	}
}

func TestAMemberThatLeavesMidClaimDoesNotHoldTheOthers(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2, 3)
	defer stops(seats)
	a, c := seats[0], seats[2]

	c.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "C's hold", func() bool { return holds(a, 1, 3) })

	c.closeAll()
	await(t, "C's hold released", func() bool { return !holds(a, 1, 3) })

	a.claim.Claim(1, 30, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted && mint.Member == 1 {
			t.Log("the claim admitted")
		}
	})
	await(t, "A's claim admitted", func() bool { return holds(a, 1, 1) })
}

func TestAMemberThatJoinsLateAgreesWithTheRest(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2)
	defer stops(seats)
	codec := wireCodec(t)

	a := seats[0]
	a.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "the first hold", func() bool { return holds(a, 1, 1) })

	third := newSeat(t, dir, 3, func(int64) int { return 2 }, codec)
	defer third.stop()
	dialRoom(t, dir, third, codec)
	third.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	await(t, "the second hold", func() bool { return holds(a, 1, 3) })
	await(t, "the joiner agrees", func() bool {
		return holds(third, 1, 1) && holds(third, 1, 3) && holds(a, 1, 3)
	})
}

func TestAClaimRacingAJoinNeverYieldsTwoHolders(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 1 }, 1, 2)
	defer stops(seats)
	codec := wireCodec(t)
	a, b := seats[0], seats[1]

	block := make(chan struct{})
	b.engine.Add(evt.Func(func(context.Context) { <-block }), 91)

	a.claim.Claim(1, 30, 0, func(broadcast.Mint, bool) {})
	third := newSeat(t, dir, 3, func(int64) int { return 1 }, codec)
	defer third.stop()
	dialRoom(t, dir, third, codec)
	await(t, "the joiner live on A", func() bool { return live(a, 3) })
	time.Sleep(120 * time.Millisecond)
	if holdersCount(a, 1) != 0 {
		t.Fatal("the claim became final before every live member acknowledged it")
	}
	close(block)
	await(t, "one holder", func() bool { return holds(a, 1, 1) })
	await(t, "the joiner agrees", func() bool { return holds(third, 1, 1) })
}

func TestTheFoldNeverHoldsMoreThanThePlacesAndItsOneEntry(t *testing.T) {
	dir := shortTemp(t)
	seats := seatMembers(t, dir, func(int64) int { return 2 }, 1, 2, 3, 4)
	defer stops(seats)

	placesDone := make(chan int64, 2)
	for _, s := range seats[:2] {
		s.claim.Claim(1, 30, 0, func(mint broadcast.Mint, admitted bool) {
			if admitted {
				placesDone <- mint.Member
			}
		})
	}
	<-placesDone
	<-placesDone
	await(t, "the places", func() bool { return holdersCount(seats[0], 1) == 2 })

	entry := make(chan int64, 1)
	seats[2].claim.Claim(1, 90, 0, func(mint broadcast.Mint, admitted bool) {
		if admitted {
			entry <- mint.Member
		}
	})
	<-entry
	await(t, "the entry holder on every member", func() bool {
		for _, s := range seats {
			if holdersCount(s, 1) != 3 || !holds(s, 1, 3) {
				return false
			}
		}
		return true
	})

	seats[3].claim.Claim(1, 90, 0, func(broadcast.Mint, bool) {})
	time.Sleep(120 * time.Millisecond)
	if holdersCount(seats[0], 1) != 3 || holds(seats[0], 1, 4) {
		t.Fatalf("the second higher claim entered: %d holders", holdersCount(seats[0], 1))
	}
}
