package broadcast

import (
	"sort"

	"github.com/mrsirg97-rgb/evt"
)

type Operation interface {
	Message
	Mint() Mint
	Key() int64
	Parent() int64
	Version() int64
	Priority() int
}

type operation struct {
	Message
	mint     Mint
	key      int64
	parent   int64
	version  int64
	priority int
}

func NewOperation(mint Mint, priority int, key, parent, version int64, payload evt.Signal) Operation {
	return &operation{
		Message:  NewMessage(mint.Member, true, payload),
		mint:     mint,
		key:      key,
		parent:   parent,
		version:  version,
		priority: priority,
	}
}

func (o *operation) Mint() Mint     { return o.mint }
func (o *operation) Key() int64     { return o.key }
func (o *operation) Parent() int64  { return o.parent }
func (o *operation) Version() int64 { return o.version }
func (o *operation) Priority() int  { return o.priority }

func mintLess(a, b Mint) bool {
	if a.Step != b.Step {
		return a.Step < b.Step
	}
	if a.Member != b.Member {
		return a.Member < b.Member
	}
	return a.Nonce < b.Nonce
}

func opLess(a, b Operation) bool {
	if a.Priority() != b.Priority() {
		return a.Priority() > b.Priority()
	}
	return mintLess(a.Mint(), b.Mint())
}

func sortOp(ops []Operation) {
	sort.Slice(ops, func(i, j int) bool { return opLess(ops[i], ops[j]) })
}
