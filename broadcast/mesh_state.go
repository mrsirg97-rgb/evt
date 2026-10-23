package broadcast

import (
	"errors"
	"sort"
)

var errReleaseWithoutClaim = errors.New("broadcast: a release carries no claim")

type meshState struct {
	holds map[string]map[Mint]Act
}

func NewMeshState() State {
	return &meshState{holds: map[string]map[Mint]Act{}}
}

func (s *meshState) Next(act Act) error {
	switch act.Kind() {
	case ActClaim:
		m := s.holds[act.Machine()]
		if m == nil {
			m = map[Mint]Act{}
			s.holds[act.Machine()] = m
		}
		m[act.Mint()] = act
	case ActRelease:
		claim := act.Claim()
		if claim == nil {
			return errReleaseWithoutClaim
		}
		if m := s.holds[claim.Machine()]; m != nil {
			delete(m, claim.Mint())
			if len(m) == 0 {
				delete(s.holds, claim.Machine())
			}
		}
	}
	return nil
}

func (s *meshState) Previous(act Act) error {
	switch act.Kind() {
	case ActClaim:
		if m := s.holds[act.Machine()]; m != nil {
			delete(m, act.Mint())
			if len(m) == 0 {
				delete(s.holds, act.Machine())
			}
		}
	case ActRelease:
		claim := act.Claim()
		if claim == nil {
			return errReleaseWithoutClaim
		}
		m := s.holds[claim.Machine()]
		if m == nil {
			m = map[Mint]Act{}
			s.holds[claim.Machine()] = m
		}
		m[claim.Mint()] = claim
	}
	return nil
}

func (s *meshState) holders(key int64) []Act {
	out := []Act{}
	for _, m := range s.holds {
		for _, act := range m {
			if act.Key() == key {
				out = append(out, act)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return actLess(out[i], out[j]) })
	return out
}

func (s *meshState) holdsOf(machine string) []Act {
	out := []Act{}
	for _, act := range s.holds[machine] {
		out = append(out, act)
	}
	sort.Slice(out, func(i, j int) bool { return actLess(out[i], out[j]) })
	return out
}
