package broadcast_test

import (
	"testing"

	"github.com/mrsirg97-rgb/evt/broadcast"
)

func versioned(step uint64, key, parent, next int64) broadcast.Operation {
	return broadcast.NewOperation(broadcast.Mint{Step: step, Member: 1, Nonce: 0}, 50, key, parent, next, broadcast.Claim{})
}

func TestGuardRejectsConflictingWrite(t *testing.T) {
	g := broadcast.NewGuard()
	if !g.Admit(versioned(1, 1, 0, 1)) {
		t.Fatal("genesis write must be admitted")
	}
	first := g.Admit(versioned(2, 1, 1, 2))
	second := g.Admit(versioned(3, 1, 1, 2))
	if !first {
		t.Fatal("first writer from head must be admitted")
	}
	if second {
		t.Fatal("second writer from a stale parent must be refused (lost update prevented)")
	}
	if g.Version(1) != 2 {
		t.Fatalf("head should be 2, got %d", g.Version(1))
	}
}

func TestGuardDeterministicAcrossReplicas(t *testing.T) {
	writes := []broadcast.Operation{
		versioned(1, 7, 0, 1),
		versioned(2, 7, 1, 2),
		versioned(3, 7, 1, 3),
		versioned(4, 7, 2, 3),
	}
	a, b := broadcast.NewGuard(), broadcast.NewGuard()
	var ra, rb []bool
	for _, w := range writes {
		ra = append(ra, a.Admit(w))
		rb = append(rb, b.Admit(w))
	}
	for i := range writes {
		if ra[i] != rb[i] {
			t.Fatalf("replicas disagreed at write %d: A=%v B=%v", i, ra[i], rb[i])
		}
	}
	if !(ra[0] && ra[1] && !ra[2] && ra[3]) {
		t.Fatalf("unexpected admit pattern: %v", ra)
	}
	if a.Version(7) != 3 || b.Version(7) != 3 {
		t.Fatalf("both replicas should converge to version 3, got A=%d B=%d", a.Version(7), b.Version(7))
	}
}

func TestGuardIndependentKeys(t *testing.T) {
	g := broadcast.NewGuard()
	if !g.Admit(versioned(1, 1, 0, 1)) || !g.Admit(versioned(2, 2, 0, 1)) {
		t.Fatal("writes to distinct keys must both be admitted")
	}
}
