package broadcast

type WAL interface {
	Append(acts ...Act)
	Act(mint Mint) (Act, bool)
	Replay(since int64) []Act
	Compact(before int64) int
	Mints() []Mint
}

type wal struct {
	acts []Act
}

func NewWAL() WAL {
	return &wal{}
}

func (w *wal) Append(acts ...Act) {
	for _, act := range acts {
		if _, ok := w.Act(act.Mint()); ok {
			continue
		}
		w.acts = append(w.acts, act)
	}
}

func (w *wal) Act(mint Mint) (Act, bool) {
	for _, act := range w.acts {
		if act.Mint() == mint {
			return act, true
		}
	}
	return nil, false
}

func (w *wal) Replay(since int64) []Act {
	out := []Act{}
	for _, act := range w.acts {
		if int64(act.Tick()) > since {
			out = append(out, act)
		}
	}
	return out
}

func (w *wal) Compact(before int64) int {
	kept := w.acts[:0]
	dropped := 0
	for _, act := range w.acts {
		if int64(act.Tick()) < before {
			dropped++
			continue
		}
		kept = append(kept, act)
	}
	w.acts = kept
	return dropped
}

func (w *wal) Mints() []Mint {
	out := make([]Mint, 0, len(w.acts))
	for _, act := range w.acts {
		out = append(out, act.Mint())
	}
	return out
}
