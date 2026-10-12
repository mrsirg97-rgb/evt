package broadcast

import (
	"sort"
)

type GuardedSequencer interface {
	Apply(state State, op Operation) error
	Order() []Operation
	Horizon() uint64
	Reorgs() int
	Refused() int
}

type guardedSequencer struct {
	window       uint64
	ordered      []Operation
	applied      []Operation
	finalApplied []Operation
	finalHeads   map[int64]int64
	highest      uint64
	reorgs       int
	refused      int
}

func NewGuardedSequencer(window uint64) GuardedSequencer {
	return &guardedSequencer{window: window, finalHeads: map[int64]int64{}}
}

func (g *guardedSequencer) Apply(state State, op Operation) error {
	for _, o := range g.ordered {
		if o.Mint() == op.Mint() {
			return nil
		}
	}
	if op.Mint().Step+g.window < g.highest {
		g.refused++
		return nil
	}
	if op.Mint().Step > g.highest {
		g.highest = op.Mint().Step
	}

	position := sort.Search(len(g.ordered), func(i int) bool {
		return opLess(op, g.ordered[i])
	})
	if position != len(g.ordered) {
		g.reorgs++
	}
	g.ordered = append(g.ordered, nil)
	copy(g.ordered[position+1:], g.ordered[position:])
	g.ordered[position] = op

	if err := g.refold(state); err != nil {
		return err
	}
	g.finalize(state)
	return nil
}

func (g *guardedSequencer) finalize(state State) {
	cut := 0
	for cut < len(g.ordered) && g.ordered[cut].Mint().Step+g.window < g.highest {
		cut++
	}
	if cut == 0 {
		return
	}
	guard := NewGuardFrom(g.finalHeads)
	for _, t := range g.ordered[:cut] {
		if guard.Admit(t) {
			g.finalApplied = append(g.finalApplied, t)
		}
	}
	g.finalHeads = guard.Snapshot()
	g.ordered = append([]Operation{}, g.ordered[cut:]...)
}

func (g *guardedSequencer) refold(state State) error {
	guard := NewGuardFrom(g.finalHeads)
	g.applied = append(g.applied[:0], g.finalApplied...)
	for _, t := range g.ordered {
		if guard.Admit(t) {
			if err := state.Next(t); err != nil {
				return err
			}
			g.applied = append(g.applied, t)
		}
	}
	return nil
}

func (g *guardedSequencer) Order() []Operation {
	return append([]Operation{}, g.applied...)
}

func (g *guardedSequencer) Horizon() uint64 { return g.highest }
func (g *guardedSequencer) Reorgs() int     { return g.reorgs }
func (g *guardedSequencer) Refused() int    { return g.refused }
