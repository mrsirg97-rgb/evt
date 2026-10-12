package broadcast_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt/broadcast"
)

type churn struct {
	mu     sync.Mutex
	inside int
	most   int
	over   bool
	dead   map[int64]bool
	in     map[int64]bool
	done   int
}

func (c *churn) enter(id int64, limit int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead[id] {
		return false
	}
	c.inside++
	c.in[id] = true
	if c.inside > c.most {
		c.most = c.inside
	}
	if c.inside > limit {
		c.over = true
	}
	return true
}

func (c *churn) leave(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.in[id] {
		c.in[id] = false
		c.inside--
	}
	c.done++
}

func (c *churn) kill(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dead[id] = true
	if c.in[id] {
		c.in[id] = false
		c.inside--
	}
}

func work(s *seat, c *churn, seed int64, requests, limit int, finished chan<- int64) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < requests; i++ {
		priority := 30
		if rng.Intn(5) == 0 {
			priority = 90
		}
		got := make(chan broadcast.Mint, 1)
		s.claim.Claim(1, priority, 0, func(m broadcast.Mint, ok bool) { got <- m })
		m := <-got
		if !c.enter(s.id, limit) {
			return
		}
		time.Sleep(time.Duration(rng.Intn(300)) * time.Microsecond)
		c.leave(s.id)
		s.claim.Release(m, priority)
	}
	finished <- s.id
}

func TestARoomUnderChurnNeverOverfillsAndNeverWedges(t *testing.T) {
	churnRoom(t, 2, 12, 40)
}

func TestAOnePlaceRoomUnderChurnNeverOverfillsAndNeverWedges(t *testing.T) {
	churnRoom(t, 1, 20, 25)
}

func churnRoom(t *testing.T, size, rounds, requests int) {
	dir := shortTemp(t)
	codec := wireCodec(t)
	places := func(int64) int { return size }
	seats := seatMembers(t, dir, places, 1, 2, 3, 4, 5, 6)
	c := &churn{dead: map[int64]bool{}, in: map[int64]bool{}}
	finished := make(chan int64, 64)
	for _, s := range seats {
		go work(s, c, s.id, requests, size+1, finished)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	alive := append([]*seat{}, seats...)
	next := int64(7)
	for round := 0; round < rounds; round++ {
		time.Sleep(time.Duration(1+rng.Intn(4)) * time.Millisecond)
		victim := rng.Intn(len(alive))
		gone := alive[victim]
		alive = append(alive[:victim], alive[victim+1:]...)
		c.kill(gone.id)
		gone.closeAll()
		gone.stop()

		joiner := newSeat(t, dir, next, places, codec)
		next++
		dialRoom(t, dir, joiner, codec)
		alive = append(alive, joiner)
		go work(joiner, c, joiner.id, requests, size+1, finished)
	}
	defer stops(alive)

	waiting := map[int64]bool{}
	for _, s := range alive {
		waiting[s.id] = true
	}
	deadline := time.After(10 * time.Second)
	for len(waiting) > 0 {
		select {
		case id := <-finished:
			delete(waiting, id)
		case <-deadline:
			for _, s := range alive {
				t.Logf("seat %d: holders %v, waiting %d, live %v", s.id, holdersOf(s, 1), len(waitingOf(s, 1)), liveOf(s))
			}
			t.Fatalf("the room wedged: %d surviving members never finished their requests", len(waiting))
		}
	}
	await(t, "an empty key on every survivor", func() bool {
		for _, s := range alive {
			if holdersCount(s, 1) != 0 {
				return false
			}
		}
		return true
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.over {
		t.Fatalf("%d members were inside at once over %d places and one entry", c.most, size)
	}
	t.Logf("%d requests entered through %d deaths and %d joins; at most %d inside at once over %d places", c.done, rounds, rounds, c.most, size)
}

func waitingOf(s *seat, key int64) []broadcast.Operation {
	var out []broadcast.Operation
	probe(s, func(c broadcast.Claimant) bool { out = c.Waiting(key); return true })
	return out
}

func liveOf(s *seat) []int64 {
	var out []int64
	probe(s, func(c broadcast.Claimant) bool { out = c.Live(); return true })
	return out
}
