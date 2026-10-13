package broadcast

import (
	"errors"
	"sort"
)

var ErrStaleTick = errors.New("broadcast: act is older than the horizon window")

type State interface {
	Next(act Act) error
	Previous(act Act) error
}

type Sequencer interface {
	Apply(state State, act Act) error
	Observe(tick int64)
	Order() []Act
	Horizon() int64
	Reorgs() int
	Refused() int
}

type sequencer struct {
	window  int64
	seen    map[Mint]bool
	applied []Act
	highest int64
	reorgs  int
	refused int
}

func NewSequencer(window int64) Sequencer {
	return &sequencer{window: window, seen: map[Mint]bool{}}
}

func (s *sequencer) Apply(state State, act Act) error {
	if s.seen[act.Mint] {
		return nil
	}
	tick := int64(act.Tick())
	if tick < s.highest-s.window {
		s.refused++
		return ErrStaleTick
	}
	if tick > s.highest {
		s.highest = tick
	}
	s.seen[act.Mint] = true
	s.prune()
	position := s.position(act)
	if position == len(s.applied) {
		if err := state.Next(act); err != nil {
			return err
		}
		s.applied = append(s.applied, act)
		return nil
	}
	tail := append([]Act{}, s.applied[position:]...)
	for i := len(tail) - 1; i >= 0; i-- {
		if err := state.Previous(tail[i]); err != nil {
			return err
		}
	}
	s.applied = s.applied[:position]
	s.reorgs++
	for _, t := range append([]Act{act}, tail...) {
		if err := state.Next(t); err != nil {
			return err
		}
		s.applied = append(s.applied, t)
	}
	return nil
}

func (s *sequencer) prune() {
	floor := s.highest - s.window
	for mint := range s.seen {
		if int64(mint.Step) < floor {
			delete(s.seen, mint)
		}
	}
}

func (s *sequencer) position(act Act) int {
	return sort.Search(len(s.applied), func(i int) bool { return actLess(act, s.applied[i]) })
}

func (s *sequencer) Order() []Act {
	return append([]Act{}, s.applied...)
}

func (s *sequencer) Horizon() int64 {
	return s.highest
}

func (s *sequencer) Observe(tick int64) {
	if tick > s.highest {
		s.highest = tick
	}
}

func (s *sequencer) Reorgs() int  { return s.reorgs }
func (s *sequencer) Refused() int { return s.refused }
