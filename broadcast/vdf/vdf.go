package vdf

import (
	"crypto/sha256"
	"math/big"
)

type VDF struct {
	p     *big.Int
	nBase int64
}

type VDFProof struct {
	Y *big.Int
	L *big.Int
}

func NewVDF(p *big.Int, nBase int64) *VDF {
	return &VDF{p: p, nBase: nBase}
}

func (v *VDF) ComputeVDF(input *big.Int) (*big.Int, *VDFProof) {
	output := v.generateOutput(input)
	return output, v.generateProof(input, output)
}

func (v *VDF) VerifyVDF(output, input *big.Int, proof *VDFProof) bool {
	if output == nil || input == nil || proof == nil || proof.Y == nil || proof.L == nil {
		return false
	}
	l := hashToPrime(input, output)
	if l.Cmp(proof.L) != 0 {
		return false
	}
	r := v.calculateR(l)
	piToL := new(big.Int).Exp(proof.Y, l, v.p)
	inputToR := new(big.Int).Exp(input, r, v.p)
	expOutput := new(big.Int).Mod(new(big.Int).Mul(piToL, inputToR), v.p)
	return output.Cmp(expOutput) == 0
}

func (v *VDF) generateOutput(input *big.Int) *big.Int {
	exponent := new(big.Int).Exp(big.NewInt(2), v.calculateNs(), nil)
	return new(big.Int).Exp(input, exponent, v.p)
}

func (v *VDF) generateProof(input, output *big.Int) *VDFProof {
	l := hashToPrime(input, output)
	twoToNs := new(big.Int).Exp(big.NewInt(2), v.calculateNs(), nil)
	q := new(big.Int).Div(twoToNs, l)
	pi := new(big.Int).Exp(input, q, v.p)
	return &VDFProof{Y: pi, L: l}
}

func hashToPrime(input, output *big.Int) *big.Int {
	digest := sha256.New()
	digest.Write(input.Bytes())
	digest.Write([]byte{0x00})
	digest.Write(output.Bytes())
	candidate := new(big.Int).SetBytes(digest.Sum(nil)[:16])
	candidate.SetBit(candidate, 127, 1)
	candidate.SetBit(candidate, 0, 1)
	two := big.NewInt(2)
	for !candidate.ProbablyPrime(20) {
		candidate.Add(candidate, two)
	}
	return candidate
}

func (v *VDF) calculateNs() *big.Int {
	return big.NewInt(v.nBase)
}

func (v *VDF) calculateR(l *big.Int) *big.Int {
	return new(big.Int).Exp(big.NewInt(2), v.calculateNs(), l)
}
