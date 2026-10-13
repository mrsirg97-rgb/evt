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
	switch act.Kind {
	case ActClaim:
		m := s.holds[act.Machine]
		if m == nil {
			m = map[Mint]Act{}
			s.holds[act.Machine] = m
		}
		m[act.Mint] = act
	case ActRelease:
		if act.Claim == nil {
			return errReleaseWithoutClaim
		}
		if m := s.holds[act.Claim.Machine]; m != nil {
			delete(m, act.Claim.Mint)
			if len(m) == 0 {
				delete(s.holds, act.Claim.Machine)
			}
		}
	}
	return nil
}

func (s *meshState) Previous(act Act) error {
	switch act.Kind {
	case ActClaim:
		if m := s.holds[act.Machine]; m != nil {
			delete(m, act.Mint)
			if len(m) == 0 {
				delete(s.holds, act.Machine)
			}
		}
	case ActRelease:
		if act.Claim == nil {
			return errReleaseWithoutClaim
		}
		m := s.holds[act.Claim.Machine]
		if m == nil {
			m = map[Mint]Act{}
			s.holds[act.Claim.Machine] = m
		}
		m[act.Claim.Mint] = *act.Claim
	}
	return nil
}

func (s *meshState) holders(key int64) []Act {
	out := []Act{}
	for _, m := range s.holds {
		for _, act := range m {
			if act.Key == key {
				out = append(out, act)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return actLess(out[i], out[j]) })
	return out
}

func (s *meshState) machines() []string {
	out := make([]string, 0, len(s.holds))
	for machine := range s.holds {
		out = append(out, machine)
	}
	sort.Strings(out)
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
