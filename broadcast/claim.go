package broadcast

import (
	"context"
	"sort"

	"github.com/mrsirg97-rgb/evt"
)

type ask struct {
	op       Operation
	posted   func(mint Mint, admitted bool)
	acked    map[int64]bool
	final    bool
	proposal Operation
	votes    map[int64]bool
}

type offer struct {
	op    Operation
	voted bool
	yes   bool
}

type Claimant interface {
	Id() int64
	Subscribe(transport Transport)
	Accept(transport Transport)
	Seated() bool
	Settle()
	Claim(key int64, priority int, talk int64, posted func(mint Mint, admitted bool))
	Release(claim Mint, priority int)
	Act(payload evt.Signal, key int64, priority int, posted func(admitted bool))
	Holders(key int64) []Operation
	Waiting(key int64) []Operation
	Live() []int64
}

type claimant struct {
	engine evt.Engine
	loop   evt.Engine
	self   int64
	minter Minter
	fold   Fold
	voice  int
	told   func(evt.Notice)

	peers    map[int64]Transport
	dial     map[int64]bool
	dialTr   map[int64]Transport
	live     map[int64]bool
	seated   map[int64]bool
	dialed   bool
	ready    bool
	preQueue []func()

	pending map[Mint]*ask
	mine    map[Mint]*ask
	offers  map[Mint]*offer
	owed    map[Mint]bool
	holds   map[int64]map[Mint]bool
	behalf  map[Mint]bool

	awaiting map[int64]map[int64]bool
	heard    map[int64]map[int64]bool
}

func NewClaimant(engine, loop evt.Engine, self int64, minter Minter, fold Fold, voice int, told func(evt.Notice)) Claimant {
	return &claimant{
		engine:  engine,
		loop:    loop,
		self:    self,
		minter:  minter,
		fold:    fold,
		voice:   voice,
		told:    told,
		peers:   make(map[int64]Transport),
		dial:    make(map[int64]bool),
		dialTr:  make(map[int64]Transport),
		live:    make(map[int64]bool),
		seated:  make(map[int64]bool),
		pending: make(map[Mint]*ask),
		mine:    make(map[Mint]*ask),
		offers:  make(map[Mint]*offer),
		owed:    make(map[Mint]bool),
		holds:   make(map[int64]map[Mint]bool),
		behalf:  make(map[Mint]bool),

		awaiting: make(map[int64]map[int64]bool),
		heard:    make(map[int64]map[int64]bool),
	}
}

func (c *claimant) Id() int64 { return c.self }

func (c *claimant) post(f func()) {
	c.engine.Add(evt.Func(func(context.Context) { f() }), c.voice)
}

func (c *claimant) send(transport Transport, signal evt.Signal) {
	transport.Send(context.Background(), func(error) {}, NewMessage(c.self, true, signal))
}

func (c *claimant) Subscribe(transport Transport) {
	c.post(func() {
		peer := transport.Id()
		c.peers[peer] = transport
		c.dial[peer] = true
		c.dialTr[peer] = transport
		c.live[peer] = true
		delete(c.awaiting, peer)
		c.wire(transport)
		c.send(transport, Joined{})
		c.reevaluate()
	})
}

func (c *claimant) Accept(transport Transport) {
	c.wire(transport)
}

func (c *claimant) wire(transport Transport) {
	transport.Recv(context.Background(), func(err error, messages ...Message) {
		c.deliver(transport, err, messages...)
	})
}

func (c *claimant) deliver(transport Transport, err error, messages ...Message) {
	if err != nil {
		c.post(func() { c.closed(transport) })
		return
	}
	for _, m := range messages {
		origin := m.Origin()
		switch ev := m.Signal().(type) {
		case Acknowledgement:
			c.post(func() { c.ackArrived(origin, ev) })
		case Pinned:
			c.post(func() { c.pinArrived(ev) })
		case Withdrawn:
			c.post(func() { c.withdrawArrived(ev) })
		case Checkpoint:
			c.post(func() { c.checkpointArrived(ev) })
		case Left:
			c.post(func() { c.leftArrived(origin, ev) })
		case Joined:
			c.post(func() { c.greeted(transport, origin) })
		case Seated:
			c.post(func() { c.answered(origin, ev) })
		default:
			if op, ok := m.(Operation); ok {
				c.post(func() { c.opArrived(op) })
				continue
			}
			if n, ok := ev.(evt.Notice); ok && c.told != nil {
				c.told(n)
			}
		}
	}
}

func (c *claimant) closed(transport Transport) {
	for peer, tr := range c.peers {
		if tr == transport {
			c.left(peer)
			return
		}
	}
}

func (c *claimant) greeted(transport Transport, peer int64) {
	if c.dial[peer] {
		if c.self < peer {
			transport = c.dialTr[peer]
		} else if tr := c.dialTr[peer]; tr != nil {
			tr.Close()
		}
	}
	c.peers[peer] = transport
	c.live[peer] = true
	delete(c.awaiting, peer)
	if c.told != nil {
		c.told(evt.Notice{Source: "room", Text: "member joined"})
	}
	for _, checkpoint := range c.fold.Checkpoints() {
		c.send(transport, checkpoint)
	}
	asks := c.asks()
	for _, a := range asks {
		transport.Send(context.Background(), func(error) {}, a.op)
	}
	for _, a := range asks {
		if a.proposal != nil {
			transport.Send(context.Background(), func(error) {}, a.proposal)
		}
	}
	c.send(transport, Seated{Live: c.Live()})
	c.reevaluate()
}

func (c *claimant) answered(peer int64, seated Seated) {
	if !c.live[peer] {
		return
	}
	for _, member := range seated.Live {
		if member != c.self && !c.live[member] {
			c.await(member, peer)
		}
	}
	c.seated[peer] = true
	c.settle()
	c.reevaluate()
}

func (c *claimant) await(member, peer int64) {
	set := c.awaiting[member]
	if set == nil {
		set = make(map[int64]bool)
		c.awaiting[member] = set
	}
	set[peer] = true
}

func (c *claimant) leftArrived(peer int64, left Left) {
	if set := c.awaiting[left.Member]; set != nil {
		delete(set, peer)
		if len(set) == 0 {
			delete(c.awaiting, left.Member)
		}
	} else if c.live[left.Member] {
		set := c.heard[left.Member]
		if set == nil {
			set = make(map[int64]bool)
			c.heard[left.Member] = set
		}
		set[peer] = true
	}
	if tr := c.peers[peer]; tr != nil && !left.Answer && !c.live[left.Member] {
		c.send(tr, Left{Member: left.Member, Answer: true})
	}
	c.reevaluate()
}

func (c *claimant) settle() {
	if c.ready || !c.dialed {
		return
	}
	for peer := range c.dial {
		if !c.seated[peer] {
			return
		}
	}
	c.ready = true
	queued := c.preQueue
	c.preQueue = nil
	for _, f := range queued {
		f()
	}
	c.reevaluate()
}

func (c *claimant) Seated() bool { return c.ready }

func (c *claimant) Settle() {
	c.post(func() {
		c.dialed = true
		c.settle()
	})
}

func (c *claimant) Claim(key int64, priority int, talk int64, posted func(mint Mint, admitted bool)) {
	c.post(func() {
		c.whenSeated(func() { c.submit(key, priority, Claim{Talk: talk}, posted) })
	})
}

func (c *claimant) Act(payload evt.Signal, key int64, priority int, posted func(admitted bool)) {
	c.post(func() {
		c.whenSeated(func() {
			c.submit(key, priority, payload, func(mint Mint, admitted bool) { posted(admitted) })
		})
	})
}

func (c *claimant) whenSeated(f func()) {
	if !c.ready {
		c.preQueue = append(c.preQueue, f)
		return
	}
	f()
	c.reevaluate()
}

func (c *claimant) Release(claim Mint, priority int) {
	c.post(func() {
		for key, mints := range c.holds {
			if mints[claim] {
				c.submit(key, priority, Release{Claim: claim}, nil)
				c.reevaluate()
				return
			}
		}
	})
}

func (c *claimant) submit(key int64, priority int, payload evt.Signal, posted func(mint Mint, admitted bool)) {
	op := NewOperation(c.minter.Mint(), priority, key, 0, 0, payload)
	a := &ask{op: op, posted: posted, acked: make(map[int64]bool)}
	c.pending[op.Mint()] = a
	c.fold.Post(op)
	c.mark(a)
	c.publish(op)
}

func (c *claimant) mark(a *ask) {
	if a.final {
		return
	}
	for peer := range c.live {
		if !a.acked[peer] {
			return
		}
	}
	a.final = true
}

func (c *claimant) publish(m Message) {
	for peer := range c.live {
		if tr := c.peers[peer]; tr != nil {
			tr.Send(context.Background(), func(error) {}, m)
		}
	}
}

func (c *claimant) asks() []*ask {
	out := make([]*ask, 0, len(c.pending))
	for _, a := range c.pending {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return opLess(out[i].op, out[j].op) })
	return out
}

func (c *claimant) opArrived(op Operation) {
	if !c.live[op.Origin()] {
		return
	}
	if _, ok := op.Signal().(Admission); ok {
		c.offers[op.Mint()] = &offer{op: op}
		c.reevaluate()
		return
	}
	if !c.fold.Post(op) {
		return
	}
	c.owed[op.Mint()] = true
	c.reevaluate()
}

func (c *claimant) ackArrived(origin int64, ack Acknowledgement) {
	if a := c.mine[ack.Mint]; a != nil {
		if !ack.Applied {
			c.withdraw(a)
			c.reevaluate()
			return
		}
		a.votes[origin] = true
		c.reevaluate()
		return
	}
	if a := c.pending[ack.Mint]; a != nil {
		a.acked[origin] = true
		c.mark(a)
		c.reevaluate()
	}
}

func (c *claimant) pinArrived(pinned Pinned) {
	o := c.offers[pinned.Admission]
	delete(c.offers, pinned.Admission)
	act := c.fold.Act(pinned.Act)
	if act == nil {
		c.reevaluate()
		return
	}
	if o == nil {
		c.fold.Include(act)
	} else {
		c.fold.Pin(o.op, act)
	}
	c.sweep(act.Key())
	c.reevaluate()
}

func (c *claimant) withdrawArrived(withdrawn Withdrawn) {
	delete(c.offers, withdrawn.Admission)
	c.reevaluate()
}

func (c *claimant) checkpointArrived(checkpoint Checkpoint) {
	if c.fold.Adopt(checkpoint) {
		c.sweep(checkpoint.Key)
	}
	c.reevaluate()
}

func (c *claimant) reevaluate() {
	for {
		c.acknowledge()
		c.ballot()
		c.orphans()
		if len(c.awaiting) > 0 {
			return
		}
		moved := false
		for _, a := range c.asks() {
			if c.tally(a) {
				moved = true
			}
		}
		if c.advance() {
			moved = true
		}
		if !moved {
			return
		}
	}
}

func (c *claimant) acknowledge() {
	if len(c.owed) == 0 {
		return
	}
	for _, act := range c.fold.Acts() {
		if !c.owed[act.Mint()] {
			continue
		}
		tr := c.peers[act.Origin()]
		if tr == nil {
			continue
		}
		if _, isClaim := act.Signal().(Claim); isClaim && !c.fold.First(act) {
			continue
		}
		delete(c.owed, act.Mint())
		c.send(tr, Acknowledgement{Mint: act.Mint(), Applied: true})
	}
	for mint := range c.owed {
		if c.fold.Act(mint) == nil {
			delete(c.owed, mint)
		}
	}
}

func (c *claimant) ballot() {
	for mint, o := range c.offers {
		if o.voted {
			continue
		}
		tr := c.peers[o.op.Origin()]
		if tr == nil {
			continue
		}
		key := o.op.Key()
		head := c.fold.Head(key)
		if o.op.Parent() > head {
			continue
		}
		if o.op.Parent() < head {
			delete(c.offers, mint)
			c.send(tr, c.fold.Checkpoint(key))
			c.send(tr, Acknowledgement{Mint: mint})
			continue
		}
		o.voted = true
		o.yes = !c.outranks(o.op)
		c.send(tr, Acknowledgement{Mint: mint, Applied: o.yes})
	}
}

func (c *claimant) outranks(theirs Operation) bool {
	for _, a := range c.mine {
		if a.proposal.Key() == theirs.Key() && a.proposal.Parent() == theirs.Parent() && opLess(a.proposal, theirs) {
			return true
		}
	}
	return false
}

func (c *claimant) yielded(key, head int64) bool {
	for _, o := range c.offers {
		if o.yes && o.op.Key() == key && o.op.Parent() == head {
			return true
		}
	}
	return false
}

func (c *claimant) orphans() {
	if !c.ready {
		return
	}
	held := make(map[Mint]bool)
	for _, checkpoint := range c.fold.Checkpoints() {
		for _, h := range checkpoint.Holds {
			member := h.Claim.Member
			if member == c.self || c.live[member] {
				continue
			}
			held[h.Claim] = true
			if c.behalf[h.Claim] {
				continue
			}
			c.behalf[h.Claim] = true
			c.submit(checkpoint.Key, h.Priority, Release{Claim: h.Claim}, nil)
		}
	}
	for claim := range c.behalf {
		if !held[claim] {
			delete(c.behalf, claim)
		}
	}
}

func (c *claimant) advance() bool {
	asks := c.asks()
	busy := make(map[int64]bool)
	for _, a := range asks {
		if a.proposal != nil {
			busy[a.op.Key()] = true
		}
	}
	proposed := false
	for _, a := range asks {
		key := a.op.Key()
		if busy[key] || !a.final || !c.fold.Admits(a.op) || c.yielded(key, c.fold.Head(key)) {
			continue
		}
		busy[key] = true
		proposed = true
		c.propose(a)
	}
	return proposed
}

func (c *claimant) propose(a *ask) {
	key := a.op.Key()
	head := c.fold.Head(key)
	a.proposal = NewOperation(c.minter.Mint(), a.op.Priority(), key, head, head+1, Admission{Act: a.op.Mint()})
	a.votes = make(map[int64]bool)
	c.mine[a.proposal.Mint()] = a
	c.publish(a.proposal)
}

func (c *claimant) tally(a *ask) bool {
	if a.proposal == nil || c.pending[a.op.Mint()] != a {
		return false
	}
	for peer := range c.live {
		if !a.votes[peer] {
			return false
		}
	}
	c.pin(a)
	return true
}

func (c *claimant) pin(a *ask) {
	admission := a.proposal
	key := a.op.Key()
	if c.fold.Head(key) != admission.Parent() {
		c.withdraw(a)
		return
	}
	a.proposal = nil
	delete(c.mine, admission.Mint())
	delete(c.pending, a.op.Mint())
	c.publish(NewMessage(c.self, true, Pinned{Admission: admission.Mint(), Act: a.op.Mint()}))
	c.fold.Pin(admission, a.op)
	c.entered(a)
	c.sweep(key)
}

func (c *claimant) withdraw(a *ask) {
	admission := a.proposal
	a.proposal = nil
	delete(c.mine, admission.Mint())
	c.publish(NewMessage(c.self, true, Withdrawn{Admission: admission.Mint()}))
}

func (c *claimant) sweep(key int64) {
	head := c.fold.Head(key)
	for mint, o := range c.offers {
		if o.op.Key() == key && o.op.Parent() < head {
			delete(c.offers, mint)
		}
	}
	for _, a := range c.asks() {
		if a.proposal != nil && a.proposal.Key() == key && a.proposal.Parent() < head {
			c.withdraw(a)
		}
	}
}

func (c *claimant) entered(a *ask) {
	key := a.op.Key()
	switch payload := a.op.Signal().(type) {
	case Claim:
		if c.holds[key] == nil {
			c.holds[key] = make(map[Mint]bool)
		}
		c.holds[key][a.op.Mint()] = true
	case Release:
		delete(c.holds[key], payload.Claim)
	}
	if a.posted != nil {
		mint := a.op.Mint()
		c.loop.Add(evt.Func(func(context.Context) { a.posted(mint, true) }), a.op.Priority())
	}
}

func (c *claimant) left(peer int64) {
	if !c.live[peer] {
		return
	}
	delete(c.live, peer)
	delete(c.peers, peer)
	delete(c.dial, peer)
	delete(c.dialTr, peer)
	delete(c.seated, peer)
	c.fold.Gone(peer)
	for mint, o := range c.offers {
		if o.op.Origin() == peer {
			delete(c.offers, mint)
		}
	}
	for member, set := range c.awaiting {
		delete(set, peer)
		if len(set) == 0 {
			delete(c.awaiting, member)
		}
	}
	for _, set := range c.heard {
		delete(set, peer)
	}
	wait := make(map[int64]bool)
	for other := range c.live {
		if !c.heard[peer][other] {
			wait[other] = true
		}
	}
	delete(c.heard, peer)
	if len(wait) > 0 {
		c.awaiting[peer] = wait
	}
	for _, checkpoint := range c.fold.Checkpoints() {
		c.publish(NewMessage(c.self, true, checkpoint))
	}
	c.publish(NewMessage(c.self, true, Left{Member: peer}))
	if c.told != nil {
		c.told(evt.Notice{Source: "room", Text: "member left", Level: evt.LevelInfo})
	}
	for _, a := range c.asks() {
		c.mark(a)
	}
	c.settle()
	c.reevaluate()
}

func (c *claimant) Holders(key int64) []Operation { return c.fold.Holders(key) }

func (c *claimant) Waiting(key int64) []Operation { return c.fold.Waiting(key) }

func (c *claimant) Live() []int64 {
	out := make([]int64, 0, len(c.live))
	for peer := range c.live {
		out = append(out, peer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
