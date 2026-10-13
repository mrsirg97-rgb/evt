package broadcast

import (
	"testing"
	"time"
)

const (
	meshTestIterations = int64(40000)
	meshTestPatience   = 20 * time.Second
)

func awaitTicking(t *testing.T, meshes []Mesh) {
	t.Helper()
	await(t, "every member's clock must tick", func() bool {
		for i := range meshes {
			if meshes[i].Horizon() == 0 {
				return false
			}
		}
		return true
	})
}

func await(t *testing.T, claim string, met func() bool) {
	t.Helper()
	deadline := time.Now().Add(meshTestPatience)
	for time.Now().Before(deadline) {
		if met() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(claim)
}

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
			Iterations: meshTestIterations,
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
	awaitTicking(t, meshes)
}

func TestSilentDoorReleasesItsHolds(t *testing.T) {
	meshes := meshRoom(t, 3, nil, fullMesh(3))
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

func TestToldWaitsPastTheHorizon(t *testing.T) {
	fired := make(chan Act, 8)
	meshes := meshRoom(t, 2, func(i int, act Act) {
		if i == 1 {
			fired <- act
		}
	}, fullMesh(2))
	startAll(t, meshes)

	claim, ok := meshes[0].Post(7, 20, Mint{})
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	select {
	case act := <-fired:
		if act.Mint != claim.Mint {
			t.Fatalf("the told carried mint %v, want %v", act.Mint, claim.Mint)
		}
	case <-time.After(meshTestPatience):
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

	if _, ok := meshes[0].Post(7, 20, Mint{}); !ok {
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
	await(t, "the mesh must run well ahead of the joiner", func() bool {
		return meshes[0].Horizon() > 40 && meshes[1].Horizon() > 40
	})
	go meshes[2].Start(ctx)
	await(t, "the joiner must reach the mesh's tick", func() bool {
		at := meshes[2].Horizon()
		return at > 40 && meshes[0].Horizon()-at <= 2
	})

	var mine Act
	await(t, "the joiner must be able to post", func() bool {
		act, ok := meshes[2].Post(7, 20, Mint{})
		mine = act
		return ok
	})
	theirs, ok := meshes[0].Post(8, 20, Mint{})
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	await(t, "a claim must cross both ways between the joiner and the mesh", func() bool {
		a, b := meshes[0].Holders(7), meshes[2].Holders(8)
		return len(a) == 1 && a[0].Mint == mine.Mint && len(b) == 1 && b[0].Mint == theirs.Mint
	})
}

func TestAPostBeforeTheFirstTickIsRefused(t *testing.T) {
	m := meshRoom(t, 1, nil, fullMesh(1))[0].(*mesh)
	t.Cleanup(m.Stop)
	go m.engine.Start(m.ctx)

	if _, ok := m.Post(7, 20, Mint{}); ok {
		t.Fatal("a post with no tick under it must be refused, not reported held")
	}
	if held := len(m.Holders(7)); held != 0 {
		t.Fatalf("a refused post must not hold, holders=%d", held)
	}
}

func TestTheLogStaysInsideTheWindow(t *testing.T) {
	meshes := meshRoom(t, 1, nil, fullMesh(1))
	startAll(t, meshes)
	for i := 0; i < 200; i++ {
		claim, ok := meshes[0].Post(7, 20, Mint{})
		if !ok {
			t.Fatal("the member must be alive to post")
		}
		meshes[0].Release(claim)
	}
	m := meshes[0].(*mesh)
	await(t, "acts below the window must leave the log and the order", func() bool {
		var logged, ordered int
		m.ask(func() { logged, ordered = len(m.wal.Mints()), len(m.seq.Order()) })
		return logged == 0 && ordered == 0
	})
	if held := len(meshes[0].Holders(7)); held != 0 {
		t.Fatalf("every claim was released, holders=%d", held)
	}
}
