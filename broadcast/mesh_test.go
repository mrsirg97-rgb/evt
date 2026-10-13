package broadcast

import (
	"testing"
	"time"
)

func meshRoom(t *testing.T, count int, told func(i int, act Act), peersOf func(i int) []int) []Mesh {
	t.Helper()
	addrs := freePorts(t, count)
	meshes := make([]Mesh, count)
	for i := range meshes {
		peersAddrs := []string{}
		for _, j := range peersOf(i) {
			peersAddrs = append(peersAddrs, addrs[j])
		}
		var toldFn func(Act)
		if told != nil {
			index := i
			toldFn = func(act Act) { told(index, act) }
		}
		meshes[i] = NewMesh(int64(i)+100, MeshConfig{
			Room:       "mesh-room",
			Machine:    "machine-" + string(rune('a'+i)),
			Addr:       addrs[i],
			Peers:      peersAddrs,
			Prime:      RFC3526Prime1536(),
			Iterations: 5000,
			Timeout:    time.Second,
			Idle:       time.Second,
			Window:     4,
			Rounds:     3,
			Voice:      30,
			Told:       toldFn,
		})
	}
	return meshes
}

func startAll(t *testing.T, meshes []Mesh) {
	t.Helper()
	ctx := t.Context()
	for i := range meshes {
		go meshes[i].Start(ctx)
	}
	t.Cleanup(func() {
		for i := range meshes {
			meshes[i].Stop()
		}
	})
	time.Sleep(200 * time.Millisecond)
}

func TestSilentDoorReleasesItsHolds(t *testing.T) {
	meshes := meshRoom(t, 3, nil, fullMesh(3))
	startAll(t, meshes)

	if _, ok := meshes[0].Post(7, 20); !ok {
		t.Fatal("the member must be alive to post")
	}
	time.Sleep(100 * time.Millisecond)
	if len(meshes[1].Holders(7)) == 0 || len(meshes[2].Holders(7)) == 0 {
		t.Fatalf("the claim must cross before the silence: %d %d",
			len(meshes[1].Holders(7)), len(meshes[2].Holders(7)))
	}
	meshes[0].Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(meshes[1].Holders(7)) == 0 && len(meshes[2].Holders(7)) == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(meshes[1].Holders(7)) != 0 || len(meshes[2].Holders(7)) != 0 {
		t.Fatalf("a silent door's holds must be released: %d %d",
			len(meshes[1].Holders(7)), len(meshes[2].Holders(7)))
	}
}

func TestToldWaitsPastTheHorizon(t *testing.T) {
	fired := make(chan Act, 8)
	meshes := meshRoom(t, 2, func(i int, act Act) {
		if i == 1 {
			fired <- act
		}
	}, fullMesh(2))
	startAll(t, meshes)

	claim, ok := meshes[0].Post(7, 20)
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	select {
	case act := <-fired:
		if act.Mint != claim.Mint {
			t.Fatalf("the told carried mint %v, want %v", act.Mint, claim.Mint)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the member's told never fired past the horizon")
	}
}

func fullMesh(n int) func(int) []int {
	return func(i int) []int {
		out := []int{}
		for j := 0; j < n; j++ {
			if j != i {
				out = append(out, j)
			}
		}
		return out
	}
}

func TestPeerCutOffFromTheHomeMachineIsNeverServed(t *testing.T) {
	meshes := meshRoom(t, 3, nil, func(i int) []int {
		switch i {
		case 0:
			return []int{1}
		case 2:
			return []int{1}
		}
		return []int{0, 2}
	})
	startAll(t, meshes)

	meshes[1].Stop()
	time.Sleep(100 * time.Millisecond)

	if _, ok := meshes[0].Post(7, 20); !ok {
		t.Fatal("the member must be alive to post")
	}
	time.Sleep(500 * time.Millisecond)
	if len(meshes[2].Holders(7)) != 0 {
		t.Fatalf("a peer cut off from the home machine is never served, holders=%d", len(meshes[2].Holders(7)))
	}
}

func TestALateJoinerConverges(t *testing.T) {
	meshes := meshRoom(t, 3, nil, fullMesh(3))
	ctx := t.Context()
	t.Cleanup(func() {
		for i := range meshes {
			meshes[i].Stop()
		}
	})
	go meshes[0].Start(ctx)
	go meshes[1].Start(ctx)
	time.Sleep(time.Second)
	go meshes[2].Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && meshes[0].Horizon()-meshes[2].Horizon() > 4 {
		time.Sleep(25 * time.Millisecond)
	}
	if behind := meshes[0].Horizon() - meshes[2].Horizon(); behind > 4 {
		t.Fatalf("the joiner must reach the mesh's tick, behind by %d", behind)
	}

	var mine, theirs Act
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if act, ok := meshes[2].Post(7, 20); ok {
			mine = act
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	theirs, ok := meshes[0].Post(8, 20)
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(meshes[0].Holders(7)) == 1 && len(meshes[2].Holders(8)) == 1 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if held := meshes[0].Holders(7); len(held) != 1 || held[0].Mint != mine.Mint {
		t.Fatalf("the joiner's claim must reach the mesh, holders=%d", len(held))
	}
	if held := meshes[2].Holders(8); len(held) != 1 || held[0].Mint != theirs.Mint {
		t.Fatalf("the mesh's claim must reach the joiner, holders=%d", len(held))
	}
}

func TestAPostBeforeTheFirstTickIsRefused(t *testing.T) {
	meshes := meshRoom(t, 1, nil, fullMesh(1))
	t.Cleanup(meshes[0].Stop)
	posted := make(chan bool, 1)
	go func() {
		_, ok := meshes[0].Post(7, 20)
		posted <- ok
	}()
	time.Sleep(50 * time.Millisecond)
	go meshes[0].Start(t.Context())
	select {
	case ok := <-posted:
		if ok {
			t.Fatal("a post with no tick under it must be refused, not reported held")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the post never answered")
	}
	if held := len(meshes[0].Holders(7)); held != 0 {
		t.Fatalf("a refused post must not hold, holders=%d", held)
	}
}

func TestTheLogStaysInsideTheWindow(t *testing.T) {
	meshes := meshRoom(t, 1, nil, fullMesh(1))
	startAll(t, meshes)
	for i := 0; i < 200; i++ {
		claim, ok := meshes[0].Post(7, 20)
		if !ok {
			t.Fatal("the member must be alive to post")
		}
		meshes[0].Release(claim)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	m := meshes[0].(*mesh)
	var logged, ordered int
	m.ask(func() { logged, ordered = len(m.wal.Mints()), len(m.seq.Order()) })
	if logged != 0 || ordered != 0 {
		t.Fatalf("acts below the window must leave the log and the order, log=%d order=%d", logged, ordered)
	}
}
