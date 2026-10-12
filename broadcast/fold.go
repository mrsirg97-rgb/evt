package broadcast

import (
	"sort"
)

type Effects interface {
	Next(op Operation) error
}

type swapper interface {
	Swap() bool
}

type pin struct {
	admission Operation
	act       Operation
}

type Fold interface {
	Post(act Operation) bool
	Act(mint Mint) Operation
	Acts() []Operation
	Gone(member int64)
	First(act Operation) bool
	Admits(act Operation) bool
	Pin(admission, act Operation)
	Include(act Operation)
	Head(key int64) int64
	Checkpoint(key int64) Checkpoint
	Checkpoints() []Checkpoint
	Adopt(checkpoint Checkpoint) bool
	Holders(key int64) []Operation
	Waiting(key int64) []Operation
}

type roomFold struct {
	places func(key int64) int
	extra  Effects
	guard  Guard
	holds  map[int64]map[Mint]Hold
	acts   []Operation
	known  map[Mint]Operation
	ahead  map[int64]map[int64]pin
}

func NewFold(places func(key int64) int, extra Effects) Fold {
	return &roomFold{
		places: places,
		extra:  extra,
		guard:  NewGuard(),
		holds:  make(map[int64]map[Mint]Hold),
		known:  make(map[Mint]Operation),
		ahead:  make(map[int64]map[int64]pin),
	}
}

func (f *roomFold) Post(act Operation) bool {
	if _, ok := f.known[act.Mint()]; ok {
		return false
	}
	at := sort.Search(len(f.acts), func(i int) bool { return opLess(act, f.acts[i]) })
	f.acts = append(f.acts, nil)
	copy(f.acts[at+1:], f.acts[at:])
	f.acts[at] = act
	f.known[act.Mint()] = act
	return true
}

func (f *roomFold) Act(mint Mint) Operation {
	return f.known[mint]
}

func (f *roomFold) Acts() []Operation {
	return append([]Operation{}, f.acts...)
}

func (f *roomFold) Gone(member int64) {
	kept := f.acts[:0]
	for _, act := range f.acts {
		if act.Mint().Member == member {
			delete(f.known, act.Mint())
			continue
		}
		kept = append(kept, act)
	}
	for i := len(kept); i < len(f.acts); i++ {
		f.acts[i] = nil
	}
	f.acts = kept
	for _, slots := range f.ahead {
		for parent, p := range slots {
			if p.act.Mint().Member == member {
				delete(slots, parent)
			}
		}
	}
}

func (f *roomFold) settle(mint Mint) {
	if _, ok := f.known[mint]; !ok {
		return
	}
	delete(f.known, mint)
	for i, act := range f.acts {
		if act.Mint() == mint {
			copy(f.acts[i:], f.acts[i+1:])
			f.acts[len(f.acts)-1] = nil
			f.acts = f.acts[:len(f.acts)-1]
			return
		}
	}
}

func (f *roomFold) First(act Operation) bool {
	for _, other := range f.acts {
		if other.Key() != act.Key() || other.Mint() == act.Mint() {
			continue
		}
		if opLess(other, act) {
			return false
		}
	}
	return true
}

func (f *roomFold) Admits(act Operation) bool {
	switch payload := act.Signal().(type) {
	case Claim:
		return f.room(act) && !f.continuationWaits(act, payload)
	case swapper:
		return f.swapMay(act.Key(), payload)
	}
	return true
}

func (f *roomFold) room(claim Operation) bool {
	set := f.holds[claim.Key()]
	if placed(set) < f.count(claim.Key()) {
		return true
	}
	for _, h := range set {
		if h.Beyond || claim.Priority() <= h.Priority {
			return false
		}
	}
	return true
}

func (f *roomFold) count(key int64) int {
	if f.places == nil {
		return 0
	}
	return f.places(key)
}

func (f *roomFold) continuationWaits(act Operation, claim Claim) bool {
	key := act.Key()
	other := false
	for _, o := range f.acts {
		if o.Key() != key || o.Mint() == act.Mint() || o.Priority() != act.Priority() {
			continue
		}
		c, ok := o.Signal().(Claim)
		if ok && c.Talk != 0 && f.continues(key, c.Talk) {
			other = true
		}
	}
	return other && !(claim.Talk != 0 && f.continues(key, claim.Talk))
}

func (f *roomFold) continues(key int64, talk int64) bool {
	for _, h := range f.holds[key] {
		if h.Talk == talk {
			return true
		}
	}
	return false
}

func (f *roomFold) swapMay(key int64, swap swapper) bool {
	if len(f.holds[key]) > 0 {
		return false
	}
	if swap.Swap() {
		return true
	}
	for _, o := range f.acts {
		if o.Key() != key {
			continue
		}
		if _, ok := o.Signal().(Claim); ok && f.room(o) {
			return false
		}
	}
	return true
}

func (f *roomFold) Pin(admission, act Operation) {
	key := admission.Key()
	head := f.guard.Version(key)
	if admission.Parent() < head {
		f.Include(act)
		return
	}
	if admission.Parent() > head {
		slots := f.ahead[key]
		if slots == nil {
			slots = make(map[int64]pin)
			f.ahead[key] = slots
		}
		slots[admission.Parent()] = pin{admission: admission, act: act}
		return
	}
	f.apply(admission, act)
	f.drain(key)
}

func (f *roomFold) apply(admission, act Operation) {
	if !f.guard.Admit(admission) {
		return
	}
	key := admission.Key()
	switch payload := act.Signal().(type) {
	case Claim:
		set := f.holds[key]
		if set == nil {
			set = make(map[Mint]Hold)
			f.holds[key] = set
		}
		set[act.Mint()] = Hold{
			Claim:    act.Mint(),
			Priority: act.Priority(),
			Talk:     payload.Talk,
			Beyond:   placed(set) >= f.count(key),
		}
	case Release:
		delete(f.holds[key], payload.Claim)
	default:
		if f.extra != nil {
			_ = f.extra.Next(act)
		}
	}
	f.settle(act.Mint())
}

func (f *roomFold) Include(act Operation) {
	if _, ok := f.known[act.Mint()]; !ok {
		return
	}
	switch act.Signal().(type) {
	case Claim, Release:
	default:
		if f.extra != nil {
			_ = f.extra.Next(act)
		}
	}
	f.settle(act.Mint())
}

func (f *roomFold) drain(key int64) {
	slots := f.ahead[key]
	for len(slots) > 0 {
		head := f.guard.Version(key)
		for parent, p := range slots {
			if parent < head {
				delete(slots, parent)
				f.Include(p.act)
			}
		}
		p, ok := slots[head]
		if !ok {
			return
		}
		delete(slots, head)
		f.apply(p.admission, p.act)
	}
}

func (f *roomFold) Head(key int64) int64 {
	return f.guard.Version(key)
}

func (f *roomFold) Checkpoint(key int64) Checkpoint {
	holds := make([]Hold, 0, len(f.holds[key]))
	for _, h := range f.holds[key] {
		holds = append(holds, h)
	}
	sort.Slice(holds, func(i, j int) bool { return mintLess(holds[i].Claim, holds[j].Claim) })
	return Checkpoint{Key: key, Head: f.guard.Version(key), Holds: holds}
}

func (f *roomFold) Checkpoints() []Checkpoint {
	heads := f.guard.Snapshot()
	keys := make([]int64, 0, len(heads))
	for key, head := range heads {
		if head > 0 {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]Checkpoint, 0, len(keys))
	for _, key := range keys {
		out = append(out, f.Checkpoint(key))
	}
	return out
}

func (f *roomFold) Adopt(checkpoint Checkpoint) bool {
	key := checkpoint.Key
	if checkpoint.Head <= f.guard.Version(key) {
		return false
	}
	f.guard.Rewind(key, checkpoint.Head)
	set := make(map[Mint]Hold, len(checkpoint.Holds))
	for _, h := range checkpoint.Holds {
		set[h.Claim] = h
	}
	f.holds[key] = set
	for _, h := range checkpoint.Holds {
		f.settle(h.Claim)
	}
	f.drain(key)
	return true
}

func (f *roomFold) Holders(key int64) []Operation {
	out := make([]Operation, 0, len(f.holds[key]))
	for _, h := range f.holds[key] {
		out = append(out, NewOperation(h.Claim, h.Priority, key, 0, 0, Claim{Talk: h.Talk}))
	}
	sortOp(out)
	return out
}

func (f *roomFold) Waiting(key int64) []Operation {
	out := []Operation{}
	for _, act := range f.acts {
		if act.Key() != key {
			continue
		}
		if _, ok := act.Signal().(Claim); ok {
			out = append(out, act)
		}
	}
	return out
}

func placed(set map[Mint]Hold) int {
	n := 0
	for _, h := range set {
		if !h.Beyond {
			n++
		}
	}
	return n
}
