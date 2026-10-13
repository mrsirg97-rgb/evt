package broadcast

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	vdf "github.com/mrsirg97-rgb/evt/broadcast/vdf"
)

func TestVDFChainVerifies(t *testing.T) {
	engine := vdf.NewVDF(RFC3526Prime1536(), 5000)
	prev := GenesisSeed("test-room")
	for tick := 0; tick < 3; tick++ {
		output, proof := engine.ComputeVDF(prev)
		if !engine.VerifyVDF(output, prev, proof) {
			t.Fatalf("tick %d's proof must verify", tick)
		}
		forged := new(big.Int).Add(output, big.NewInt(1))
		if engine.VerifyVDF(forged, prev, proof) {
			t.Fatalf("tick %d's forged output must fail", tick)
		}
		prev = output
	}
}

func TestVDFClockAdvancesAndRefusesAForgedTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := evt.NewEngine()
	go engine.Start(ctx)

	clock := NewVDFClock(RFC3526Prime1536(), 5000, GenesisSeed("test-room"), 4, 30)
	clock.Start(ctx, engine, nil)

	ask := func(f func()) {
		done := make(chan struct{})
		engine.Add(evt.Func(func(context.Context) { f(); close(done) }), 30)
		<-done
	}
	deadline := time.Now().Add(3 * time.Second)
	var step uint64
	for step < 3 && time.Now().Before(deadline) {
		ask(func() { step = clock.Step() })
		time.Sleep(10 * time.Millisecond)
	}
	if step < 3 {
		t.Fatalf("the chain must advance, index=%d", step)
	}
	var own bool
	var forgedRefused bool
	ask(func() {
		own = clock.Record(clock.Step(), clock.Output(), clock.Proof())
		forged := new(big.Int).Add(clock.Output(), big.NewInt(1))
		forgedRefused = !clock.Record(clock.Step()+1, forged, clock.Proof())
	})
	if !own {
		t.Fatal("the chain's own tick must verify")
	}
	if !forgedRefused {
		t.Fatal("a forged tick must be refused")
	}
}

func TestGenesisSeedBindsTheChain(t *testing.T) {
	if GenesisSeed("room-a").Cmp(GenesisSeed("room-a")) != 0 {
		t.Fatal("the same room must derive the same seed")
	}
	if GenesisSeed("room-a").Cmp(GenesisSeed("room-b")) == 0 {
		t.Fatal("different rooms must derive different seeds")
	}
}

func TestMeshWireRoundTrips(t *testing.T) {
	encoder := MeshCodec()
	claim := Act{
		Mint:     Mint{Step: 9, Member: 42, Nonce: 7},
		Key:      5,
		Priority: 20,
		Machine:  "battlestation",
		Kind:     ActClaim,
		Output:   big.NewInt(970971),
		Proof:    &vdf.VDFProof{Y: big.NewInt(11), L: big.NewInt(13)},
	}
	act := Act{
		Mint:     Mint{Step: 10, Member: 42, Nonce: 8},
		Key:      5,
		Priority: 20,
		Machine:  "battlestation",
		Kind:     ActRelease,
		Claim:    &claim,
		Output:   big.NewInt(970972),
		Proof:    &vdf.VDFProof{Y: big.NewInt(11), L: big.NewInt(13)},
	}
	frame, err := encoder.Encode(NewMessage(42, true, act))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := encoder.Decode(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := decoded.Signal().(Act)
	if !ok {
		t.Fatalf("the decoded frame must carry an act, got %T", decoded.Signal())
	}
	if got.Mint != act.Mint || got.Key != act.Key || got.Machine != act.Machine || got.Kind != act.Kind {
		t.Fatalf("the act's fields must survive the wire: %+v", got)
	}
	if got.Claim == nil || got.Claim.Mint != claim.Mint {
		t.Fatal("a release must carry its claim through the wire")
	}
	if got.Output.Cmp(act.Output) != 0 {
		t.Fatal("the tick's output must survive the wire")
	}
	if got.Proof.L.Cmp(act.Proof.L) != 0 || got.Proof.Y.Cmp(act.Proof.Y) != 0 {
		t.Fatal("the proof must survive the wire")
	}

	tick := Tick{Index: 3, Output: big.NewInt(970973), Proof: &vdf.VDFProof{Y: big.NewInt(11), L: big.NewInt(13)}}
	frame, err = encoder.Encode(NewMessage(42, true, tick))
	if err != nil {
		t.Fatalf("encode tick: %v", err)
	}
	decoded, err = encoder.Decode(frame)
	if err != nil {
		t.Fatalf("decode tick: %v", err)
	}
	gotTick, ok := decoded.Signal().(Tick)
	if !ok {
		t.Fatalf("the decoded frame must carry a tick, got %T", decoded.Signal())
	}
	if gotTick.Index != tick.Index || gotTick.Output.Cmp(tick.Output) != 0 {
		t.Fatalf("the tick must survive the wire: %+v", gotTick)
	}
}

func TestVDFClockTrustsOnlyPastAGap(t *testing.T) {
	prime := RFC3526Prime1536()
	seed := GenesisSeed("trust-room")
	chain := vdf.NewVDF(prime, 2000)
	outputs := []*big.Int{seed}
	proofs := []*vdf.VDFProof{nil}
	for i := 0; i < 6; i++ {
		output, proof := chain.ComputeVDF(outputs[i])
		outputs = append(outputs, output)
		proofs = append(proofs, proof)
	}
	clock := NewVDFClock(prime, 2000, seed, 4, 30)

	if clock.Trust(1, big.NewInt(12345), proofs[1]) {
		t.Fatal("a tick whose predecessor is known must be verified, never trusted")
	}
	if !clock.Trust(4, outputs[4], proofs[4]) {
		t.Fatal("a member behind a gap trusts one entry")
	}
	if clock.Record(5, big.NewInt(12345), proofs[5]) {
		t.Fatal("a forged tick after the trusted entry must be refused")
	}
	if !clock.Record(5, outputs[5], proofs[5]) || clock.Step() != 5 {
		t.Fatalf("every tick after the trusted entry verifies, step=%d", clock.Step())
	}
	if clock.Trust(3, outputs[3], proofs[3]) {
		t.Fatal("the clock never trusts backwards")
	}
}
