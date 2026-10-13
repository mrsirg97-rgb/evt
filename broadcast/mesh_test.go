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
