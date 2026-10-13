package broadcast

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func freePorts(t *testing.T, count int) []string {
	t.Helper()
	out := make([]string, count)
	for i := range out {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		out[i] = listener.Addr().String()
		listener.Close()
	}
	return out
}

func TestMeshConvergesOverTheWire(t *testing.T) {
	const peers = 3
	const rounds = 4
	const iterations = meshTestIterations

	addrs := freePorts(t, peers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meshes := make([]Mesh, peers)
	for i := range meshes {
		peersAddrs := []string{}
		for j, addr := range addrs {
			if j != i {
				peersAddrs = append(peersAddrs, addr)
			}
		}
		meshes[i] = NewMesh(int64(i)+1000, MeshConfig{
			Room:       "capstone-room",
			Machine:    fmt.Sprintf("machine-%d", i),
			Addr:       addrs[i],
			Peers:      peersAddrs,
			Prime:      RFC3526Prime1536(),
			Iterations: iterations,
			Timeout:    time.Second,
			Idle:       time.Second,
			Window:     4,
			Rounds:     3,
			Voice:      30,
		})
	}
	for i := range meshes {
		go meshes[i].Start(ctx)
	}
	awaitTicking(t, meshes)

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

	deadline := time.Now().Add(meshTestPatience)
	for time.Now().Before(deadline) {
		if converged(t, meshes) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !converged(t, meshes) {
		t.Fatal("the mesh did not converge on one order")
	}
	for _, key := range []int64{100, 101, 102} {
		holders := meshes[0].Holders(key)
		if len(holders) != rounds {
			t.Fatalf("key %d holds %d claims, want %d", key, len(holders), rounds)
		}
		member := holders[0].Mint.Member
		for _, act := range holders {
			if act.Mint.Member != member {
				t.Fatalf("key %d carries two members: %d and %d", key, act.Mint.Member, member)
			}
		}
	}
	for i := range meshes {
		meshes[i].Stop()
	}
}

func converged(t *testing.T, meshes []Mesh) bool {
	t.Helper()
	for key := int64(100); key < int64(100+len(meshes)); key++ {
		reference := meshes[0].Holders(key)
		if len(reference) == 0 {
			return false
		}
		for i := 1; i < len(meshes); i++ {
			other := meshes[i].Holders(key)
			if len(other) != len(reference) {
				return false
			}
			for j := range reference {
				if other[j].Mint != reference[j].Mint {
					return false
				}
			}
		}
	}
	return true
}
