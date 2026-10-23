package broadcast

import (
	"encoding/json"
	"math/big"

	"github.com/mrsirg97-rgb/evt"
	vdf "github.com/mrsirg97-rgb/evt/broadcast/vdf"
)

type ActKind string

const (
	ActClaim   ActKind = "claim"
	ActRelease ActKind = "release"
)

type Act interface {
	evt.Signal
	Key() int64
	Priority() int
	Mint() Mint
	Kind() ActKind
	Machine() string
	Claim() Act
	Output() *big.Int
	Proof() *vdf.VDFProof
	Tick() uint64
}

type act struct {
	evt.Mark
	mint     Mint
	key      int64
	priority int
	machine  string
	kind     ActKind
	claim    Act
	output   *big.Int
	proof    *vdf.VDFProof
}

type actWire struct {
	Mint     Mint
	Key      int64
	Priority int
	Machine  string
	Kind     ActKind
	Claim    *actWire
	Output   *big.Int
	Proof    *vdf.VDFProof
}

type Tick struct {
	evt.Mark
	Index  uint64
	Output *big.Int
	Proof  *vdf.VDFProof
}

func NewAct(mint Mint, key int64, priority int, machine string, kind ActKind, claim Act, output *big.Int, proof *vdf.VDFProof) Act {
	return &act{mint: mint, key: key, priority: priority, machine: machine, kind: kind, claim: claim, output: output, proof: proof}
}

func (a *act) Key() int64           { return a.key }
func (a *act) Priority() int        { return a.priority }
func (a *act) Mint() Mint           { return a.mint }
func (a *act) Kind() ActKind        { return a.kind }
func (a *act) Machine() string      { return a.machine }
func (a *act) Claim() Act           { return a.claim }
func (a *act) Output() *big.Int     { return a.output }
func (a *act) Proof() *vdf.VDFProof { return a.proof }
func (a *act) Tick() uint64         { return a.mint.Step }

func MeshCodec() Encoder {
	reg, err := Declare[*act](NewRegistry(), "mesh/act", encodeAct, decodeAct)
	if err != nil {
		panic("broadcast: " + err.Error())
	}
	reg, err = jsonCodec[Tick](reg, "mesh/tick")
	if err != nil {
		panic("broadcast: " + err.Error())
	}
	return NewJSONEncoder(reg)
}

func actLess(a, b Act) bool {
	if a.Priority() != b.Priority() {
		return a.Priority() > b.Priority()
	}
	if a.Mint().Step != b.Mint().Step {
		return a.Mint().Step < b.Mint().Step
	}
	if a.Kind() != b.Kind() {
		return a.Kind() == ActClaim
	}
	return mintLess(a.Mint(), b.Mint())
}

func actFromWire(w *actWire) Act {
	var claim Act
	if w.Claim != nil {
		claim = actFromWire(w.Claim)
	}
	return NewAct(w.Mint, w.Key, w.Priority, w.Machine, w.Kind, claim, w.Output, w.Proof)
}

func actToWire(a Act) *actWire {
	w := &actWire{Mint: a.Mint(), Key: a.Key(), Priority: a.Priority(), Machine: a.Machine(), Kind: a.Kind(), Output: a.Output(), Proof: a.Proof()}
	if claim := a.Claim(); claim != nil {
		w.Claim = actToWire(claim)
	}
	return w
}

func decodeAct(raw []byte) (*act, error) {
	var w actWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, err
	}
	return actFromWire(&w).(*act), nil
}

func encodeAct(a *act) ([]byte, error) {
	return json.Marshal(actToWire(a))
}
