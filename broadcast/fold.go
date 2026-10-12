package broadcast

import (
	"sort"
)

type Effects interface {
	Next(op Operation) error
	Previous(op Operation) error
}

type hold struct {
	member   int64
	talk     int64
	priority int
	beyond   bool
}

type Fold interface {
	Post(op Operation) error
	Head(key int64) int64
	Log() []Operation
	Settled(mint Mint) bool
	Admitted(mint Mint) bool
	Gone(member int64)
	Held(member int64) []Operation
	Wants(op Operation) bool
	First(op Operation) bool
	Waiting(key int64) []Operation
	Holders(key int64) []Operation
	Next(op Operation) error
	Previous(op Operation) error
}

type swapper interface {
	Swap() bool
}

type roomFold struct {
	seq     Sequencer
	places  func(key int64) int
	extra   Effects
	byMint  map[Mint]Operation
	holders map[int64]map[Mint]hold
	heads   map[int64]int64
	settled map[Mint]bool
	applied map[Mint]bool
	gone    map[int64]bool
}

func NewFold(places func(key int64) int, extra Effects) Fold {
	return &roomFold{
		seq:     NewSequencer(1 << 62),
		places:  places,
		extra:   extra,
		byMint:  make(map[Mint]Operation),
		holders: make(map[int64]map[Mint]hold),
		heads:   make(map[int64]int64),
		settled: make(map[Mint]bool),
		applied: make(map[Mint]bool),
		gone:    make(map[int64]bool),
	}
}

func (f *roomFold) Post(op Operation) error {
	if err := f.seq.Apply(f, op); err != nil {
		return err
	}
	f.rebuild()
	return nil
}

func (f *roomFold) rebuild() {
	byMint := make(map[Mint]Operation)
	settled := make(map[Mint]bool)
	applied := make(map[Mint]bool)
	prevSettled := f.settled
	holders := make(map[int64]map[Mint]hold)
	heads := make(map[int64]int64)
	log := f.seq.Order()
	for _, op := range log {
		byMint[op.Mint()] = op
		if f.gone[op.Mint().Member] {
			settled[op.Mint()] = true
		}
	}
	for progressed := true; progressed; {
		progressed = false
		for _, op := range log {
			if settled[op.Mint()] || applied[op.Mint()] {
				continue
			}
			ad, isAd := op.Signal().(Admission)
			if !isAd {
				continue
			}
			act := byMint[ad.Act]
			if act == nil || settled[ad.Act] {
				continue
			}
			key := op.Key()
			head := heads[key]
			if head < op.Parent() {
				continue
			}
			if head > op.Parent() {
				settled[op.Mint()] = true
				settled[ad.Act] = true
				continue
			}
			if !f.admits(key, act, holders, prevSettled) {
				continue
			}
			heads[key] = op.Version()
			applied[op.Mint()] = true
			settled[op.Mint()] = true
			settled[ad.Act] = true
			if !f.place(key, act, ad, holders) && f.extra != nil {
				_ = f.extra.Next(act)
			}
			progressed = true
		}
	}
	f.byMint = byMint
	f.settled = settled
	f.applied = applied
	f.holders = holders
	f.heads = heads
}

func (f *roomFold) admits(key int64, act Operation, holders map[int64]map[Mint]hold, settled map[Mint]bool) bool {
	claim, isClaim := act.Signal().(Claim)
	if !isClaim {
		if _, isSwap := act.Signal().(swapper); isSwap {
			return f.swapMay(key, act, holders, settled)
		}
		return true
	}
	places := 0
	if f.places != nil {
		places = f.places(key)
	}
	set := holders[key]
	if placed(set) < places {
		return !f.continuationWaits(key, act, claim, holders, settled)
	}
	for _, h := range set {
		if h.beyond || act.Priority() <= h.priority {
			return false
		}
	}
	return !f.continuationWaits(key, act, claim, holders, settled)
}

func (f *roomFold) continuationWaits(key int64, act Operation, claim Claim, holders map[int64]map[Mint]hold, settled map[Mint]bool) bool {
	other := false
	for _, o := range f.seq.Order() {
		if o.Key() != key || o.Mint() == act.Mint() || settled[o.Mint()] {
			continue
		}
		c, ok := o.Signal().(Claim)
		if !ok || o.Priority() != act.Priority() {
			continue
		}
		if c.Talk != 0 && f.continues(key, c.Talk, holders) {
			other = true
		}
	}
	return other && !(claim.Talk != 0 && f.continues(key, claim.Talk, holders))
}

func (f *roomFold) continues(key int64, talk int64, holders map[int64]map[Mint]hold) bool {
	for _, h := range holders[key] {
		if h.talk == talk {
			return true
		}
	}
	return false
}

func (f *roomFold) swapMay(key int64, act Operation, holders map[int64]map[Mint]hold, settled map[Mint]bool) bool {
	if len(holders[key]) > 0 {
		return false
	}
	sw, ok := act.Signal().(swapper)
	if !ok || sw.Swap() {
		return true
	}
	return !f.sendable(key, holders, settled)
}

func (f *roomFold) sendable(key int64, holders map[int64]map[Mint]hold, settled map[Mint]bool) bool {
	for _, o := range f.seq.Order() {
		if o.Key() != key || settled[o.Mint()] {
			continue
		}
		if _, ok := o.Signal().(Claim); ok && f.would(o, holders) {
			return true
		}
	}
	return false
}

func (f *roomFold) would(claim Operation, holders map[int64]map[Mint]hold) bool {
	places := 0
	if f.places != nil {
		places = f.places(claim.Key())
	}
	set := holders[claim.Key()]
	if placed(set) < places {
		return true
	}
	for _, h := range set {
		if h.beyond || claim.Priority() <= h.priority {
			return false
		}
	}
	return true
}

func (f *roomFold) place(key int64, act Operation, ad Admission, holders map[int64]map[Mint]hold) bool {
	switch a := act.Signal().(type) {
	case Claim:
		set := holders[key]
		if set == nil {
			set = make(map[Mint]hold)
			holders[key] = set
		}
		set[ad.Act] = hold{
			member:   act.Mint().Member,
			talk:     a.Talk,
			priority: act.Priority(),
			beyond:   f.full(key, holders),
		}
		return true
	case Release:
		delete(holders[key], a.Claim)
		return true
	}
	return false
}

func (f *roomFold) full(key int64, holders map[int64]map[Mint]hold) bool {
	if f.places == nil {
		return false
	}
	return placed(holders[key]) >= f.places(key)
}

func placed(set map[Mint]hold) int {
	n := 0
	for _, h := range set {
		if !h.beyond {
			n++
		}
	}
	return n
}

func (f *roomFold) Head(key int64) int64 {
	return f.heads[key]
}

func (f *roomFold) Log() []Operation {
	return f.seq.Order()
}

func (f *roomFold) Settled(mint Mint) bool {
	return f.settled[mint]
}

func (f *roomFold) Admitted(mint Mint) bool {
	return f.applied[mint]
}

func (f *roomFold) Gone(member int64) {
	f.gone[member] = true
	f.rebuild()
}

func (f *roomFold) Held(member int64) []Operation {
	out := []Operation{}
	for _, set := range f.holders {
		for mint, h := range set {
			if h.member != member {
				continue
			}
			if op := f.byMint[mint]; op != nil {
				out = append(out, op)
			}
		}
	}
	sortOp(out)
	return out
}

func (f *roomFold) Wants(op Operation) bool {
	ad, ok := op.Signal().(Admission)
	if !ok {
		return true
	}
	act := f.byMint[ad.Act]
	if act == nil {
		return true
	}
	return f.admits(op.Key(), act, f.holders, f.settled)
}

func (f *roomFold) First(op Operation) bool {
	for _, o := range f.seq.Order() {
		if o.Key() != op.Key() || o.Mint() == op.Mint() || f.settled[o.Mint()] {
			continue
		}
		if opLess(o, op) {
			return false
		}
	}
	return true
}

func (f *roomFold) Waiting(key int64) []Operation {
	out := []Operation{}
	for _, o := range f.seq.Order() {
		if o.Key() == key && !f.settled[o.Mint()] {
			if _, ok := o.Signal().(Claim); ok {
				out = append(out, o)
			}
		}
	}
	sortOp(out)
	return out
}

func (f *roomFold) Holders(key int64) []Operation {
	out := []Operation{}
	for mint := range f.holders[key] {
		if op := f.byMint[mint]; op != nil {
			out = append(out, op)
		}
	}
	sortOp(out)
	return out
}

func (f *roomFold) Next(op Operation) error {
	if f.byMint != nil {
		f.byMint[op.Mint()] = op
	}
	return nil
}

func (f *roomFold) Previous(op Operation) error {
	return nil
}

func sortOp(ops []Operation) {
	sort.Slice(ops, func(i, j int) bool { return opLess(ops[i], ops[j]) })
}
