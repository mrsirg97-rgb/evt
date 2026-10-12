package broadcast

import (
	"sort"
)

type State interface {
	Next(op Operation) error
	Previous(op Operation) error
}

type Sequencer interface {
	Apply(state State, op Operation) error
	Order() []Operation
	Horizon() uint64
	Reorgs() int
	Refused() int
}

type sequencer struct {
	window  uint64
	applied []Operation
	seen    map[Mint]bool
	highest uint64
	reorgs  int
	refused int
}

func NewSequencer(window uint64) Sequencer {
	return &sequencer{window: window, seen: make(map[Mint]bool)}
}

func opLess(a, b Operation) bool {
	if a.Priority() != b.Priority() {
		return a.Priority() > b.Priority()
	}
	am, bm := a.Mint(), b.Mint()
	if am.Step != bm.Step {
		return am.Step < bm.Step
	}
	if am.Member != bm.Member {
		return am.Member < bm.Member
	}
	return am.Nonce < bm.Nonce
}

func (s *sequencer) Apply(state State, op Operation) error {
	if s.seen[op.Mint()] {
		return nil
	}
	if op.Mint().Step+s.window < s.highest {
		s.refused++
		return nil
	}
	if op.Mint().Step > s.highest {
		s.highest = op.Mint().Step
	}
	s.seen[op.Mint()] = true

	position := s.position(op)
	if position == len(s.applied) {
		if err := state.Next(op); err != nil {
			return err
		}
		s.applied = append(s.applied, op)
		return nil
	}

	tail := append([]Operation{}, s.applied[position:]...)
	for i := len(tail) - 1; i >= 0; i-- {
		if err := state.Previous(tail[i]); err != nil {
			return err
		}
	}
	s.applied = s.applied[:position]
	s.reorgs++

	for _, t := range append([]Operation{op}, tail...) {
		if err := state.Next(t); err != nil {
			return err
		}
		s.applied = append(s.applied, t)
	}
	return nil
}

func (s *sequencer) position(op Operation) int {
	return sort.Search(len(s.applied), func(i int) bool {
		return opLess(op, s.applied[i])
	})
}

func (s *sequencer) Order() []Operation {
	return append([]Operation{}, s.applied...)
}

func (s *sequencer) Horizon() uint64 { return s.highest }
func (s *sequencer) Reorgs() int     { return s.reorgs }
func (s *sequencer) Refused() int    { return s.refused }
