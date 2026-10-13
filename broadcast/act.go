package broadcast

import (
	"math/big"

	"github.com/mrsirg97-rgb/evt"
	vdf "github.com/mrsirg97-rgb/evt/broadcast/vdf"
)

type ActKind string

const (
	ActClaim   ActKind = "claim"
	ActRelease ActKind = "release"
)

type Act struct {
	evt.Mark
	Mint     Mint
	Key      int64
	Priority int
	Machine  string
	Kind     ActKind
	Claim    *Act
	Output   *big.Int
	Proof    *vdf.VDFProof
}

func (a Act) Tick() uint64 { return a.Mint.Step }

func actLess(a, b Act) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return mintLess(a.Mint, b.Mint)
}

type Tick struct {
	evt.Mark
	Index  uint64
	Output *big.Int
	Proof  *vdf.VDFProof
}

func MeshCodec() Encoder {
	reg, err := jsonCodec[Act](NewRegistry(), "mesh/act")
	if err != nil {
		panic("broadcast: " + err.Error())
	}
	reg, err = jsonCodec[Tick](reg, "mesh/tick")
	if err != nil {
		panic("broadcast: " + err.Error())
	}
	return NewJSONEncoder(reg)
}
