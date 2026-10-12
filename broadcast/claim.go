package broadcast

import (
	"context"
	"sort"

	"github.com/mrsirg97-rgb/evt"
)

type ask struct {
	op        Operation
	acked     map[int64]bool
	posted    func(mint Mint, admitted bool)
	admission Operation
	adAcked   map[int64]bool
	adFailed  bool
	final     bool
	admitted  bool
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
	ready    bool
	pending  map[Mint]*ask
	withheld map[Mint]Operation
	holds    map[int64]map[Mint]bool
	preQueue []func()
}

func NewClaimant(engine, loop evt.Engine, self int64, minter Minter, fold Fold, voice int, told func(evt.Notice)) Claimant {
	return &claimant{
		engine:   engine,
		loop:     loop,
		self:     self,
		minter:   minter,
		fold:     fold,
		voice:    voice,
		told:     told,
		peers:    make(map[int64]Transport),
		dial:     make(map[int64]bool),
		dialTr:   make(map[int64]Transport),
		live:     make(map[int64]bool),
		seated:   make(map[int64]bool),
		pending:  make(map[Mint]*ask),
		withheld: make(map[Mint]Operation),
		holds:    make(map[int64]map[Mint]bool),
	}
}

func (c *claimant) Id() int64 { return c.self }

func (c *claimant) post(priority int, f func()) {
	c.engine.Add(evt.Func(func(context.Context) { f() }), priority)
}

func (c *claimant) Subscribe(transport Transport) {
	c.wire(transport)
	transport.Send(context.Background(), func(error) {}, NewMessage(c.self, true, Joined{}))
	c.post(c.voice, func() {
		peer := transport.Id()
		c.peers[peer] = transport
		c.dial[peer] = true
		c.dialTr[peer] = transport
		c.live[peer] = true
		c.settle()
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
		c.post(c.voice, func() { c.closed(transport) })
		return
	}
	for _, m := range messages {
		switch ev := m.Signal().(type) {
		case Acknowledgement:
			c.post(ev.Priority, func() { c.ackArrived(m.Origin(), ev.Mint, ev.Applied) })
		case Joined:
			peer := m.Origin()
			c.post(c.voice, func() { c.greeted(transport, peer) })
		case Seated:
			c.post(c.voice, func() { c.answered(m.Origin()) })
		default:
			if op, ok := m.(Operation); ok {
				c.post(op.Priority(), func() { c.opArrived(op) })
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
	if c.told != nil {
		c.told(evt.Notice{Source: "room", Text: "member joined"})
	}
	for _, op := range c.fold.Log() {
		transport.Send(context.Background(), func(error) {}, op)
	}
	transport.Send(context.Background(), func(error) {}, NewMessage(c.self, true, Seated{}))
	c.reevaluate()
}

func (c *claimant) answered(peer int64) {
	c.seated[peer] = true
	c.settle()
}

func (c *claimant) settle() {
	if c.ready {
		return
	}
	for peer := range c.dial {
		if !c.seated[peer] {
			return
		}
	}
	c.ready = true
	for _, f := range c.preQueue {
		f()
	}
	c.preQueue = nil
}

func (c *claimant) Seated() bool { return c.ready }

func (c *claimant) Settle() { c.post(c.voice, func() { c.settle() }) }

func (c *claimant) Claim(key int64, priority int, talk int64, posted func(mint Mint, admitted bool)) {
	c.post(c.voice, func() {
		if !c.ready {
			c.preQueue = append(c.preQueue, func() { c.mint(key, priority, Claim{Talk: talk}, posted) })
			return
		}
		c.mint(key, priority, Claim{Talk: talk}, posted)
	})
}

func (c *claimant) Act(payload evt.Signal, key int64, priority int, posted func(admitted bool)) {
	c.post(c.voice, func() {
		if !c.ready {
			c.preQueue = append(c.preQueue, func() {
				c.mint(key, priority, payload, func(mint Mint, admitted bool) { posted(admitted) })
			})
			return
		}
		c.mint(key, priority, payload, func(mint Mint, admitted bool) { posted(admitted) })
	})
}

func (c *claimant) Release(claim Mint, priority int) {
	c.post(priority, func() {
		key, ok := int64(0), false
		for k, mints := range c.holds {
			if mints[claim] {
				key, ok = k, true
			}
		}
		if !ok {
			return
		}
		parent := c.fold.Head(key)
		op := NewOperation(c.minter.Mint(), priority, key, parent, parent+1, Release{Claim: claim})
		a := &ask{op: op, acked: make(map[int64]bool)}
		c.pending[op.Mint()] = a
		c.mark(a)
		c.publish(op)
		_ = c.fold.Post(op)
		c.reevaluate()
	})
}

func (c *claimant) mint(key int64, priority int, payload evt.Signal, posted func(mint Mint, admitted bool)) {
	parent := c.fold.Head(key)
	op := NewOperation(c.minter.Mint(), priority, key, parent, parent+1, payload)
	a := &ask{op: op, acked: make(map[int64]bool), posted: posted}
	c.pending[op.Mint()] = a
	c.mark(a)
	c.publish(op)
	_ = c.fold.Post(op)
	c.reevaluate()
}

func (c *claimant) mark(a *ask) {
	if !a.final && c.finality(a) {
		a.final = true
	}
}

func (c *claimant) publish(op Operation) {
	for peer := range c.live {
		if tr := c.peers[peer]; tr != nil {
			tr.Send(context.Background(), func(ack error) {
				if ack != nil {
					c.post(c.voice, func() { c.left(peer) })
				}
			}, op)
		}
	}
}

func (c *claimant) opArrived(op Operation) {
	_ = c.fold.Post(op)
	c.reevaluate()
	if c.mayAck(op) {
		c.sendAck(op)
		return
	}
	c.withheld[op.Mint()] = op
}

func (c *claimant) mayAck(op Operation) bool {
	if _, isClaim := op.Signal().(Claim); isClaim {
		if c.fold.Settled(op.Mint()) {
			return false
		}
		return c.fold.First(op)
	}
	return true
}

func (c *claimant) sendAck(op Operation) {
	tr := c.peers[op.Origin()]
	if tr == nil {
		c.withheld[op.Mint()] = op
		return
	}
	applied := true
	if _, isAd := op.Signal().(Admission); isAd {
		applied = c.fold.Admitted(op.Mint())
	}
	tr.Send(context.Background(), func(error) {}, NewMessage(c.self, true, Acknowledgement{Mint: op.Mint(), Priority: op.Priority(), Applied: applied}))
}

func (c *claimant) ackArrived(origin int64, mint Mint, applied bool) {
	a := c.pending[mint]
	if a == nil {
		for _, other := range c.pending {
			if other.admission != nil && other.admission.Mint() == mint {
				a = other
				break
			}
		}
		if a == nil {
			return
		}
		if a.adAcked != nil {
			a.adAcked[origin] = true
			if !applied {
				a.adFailed = true
			}
		}
		c.reevaluate()
		return
	}
	a.acked[origin] = true
	if c.finality(a) {
		a.final = true
	}
	c.reevaluate()
}

func (c *claimant) finality(a *ask) bool {
	for peer := range c.live {
		if !a.acked[peer] {
			return false
		}
	}
	return true
}

func (c *claimant) reevaluate() {
	mints := make([]Mint, 0, len(c.withheld))
	for mint := range c.withheld {
		mints = append(mints, mint)
	}
	for _, mint := range mints {
		op := c.withheld[mint]
		if c.mayAck(op) && c.peers[op.Origin()] != nil {
			delete(c.withheld, mint)
			c.sendAck(op)
		}
	}
	mints = make([]Mint, 0, len(c.pending))
	for mint := range c.pending {
		mints = append(mints, mint)
	}
	for _, mint := range mints {
		if a := c.pending[mint]; a != nil {
			c.evaluate(a)
		}
	}
}

func (c *claimant) evaluate(a *ask) {
	if a.admitted {
		return
	}
	if !a.final {
		return
	}
	key := a.op.Key()
	if a.admission == nil {
		ad := NewOperation(c.minter.Mint(), a.op.Priority(), key, a.op.Parent(), a.op.Version(), Admission{Act: a.op.Mint()})
		if !c.fold.Wants(ad) {
			return
		}
		a.admission = ad
		a.adAcked = make(map[int64]bool)
		c.publish(ad)
		_ = c.fold.Post(ad)
		c.reevaluate()
		if a.admitted {
			return
		}
	}
	if a.admission != nil {
		if !c.fold.Admitted(a.admission.Mint()) {
			if a.op.Parent() < c.fold.Head(key) {
				delete(c.pending, a.op.Mint())
				c.remint(a)
			}
			return
		}
		for peer := range c.live {
			if !a.adAcked[peer] {
				return
			}
		}
		delete(c.pending, a.op.Mint())
		if a.adFailed {
			c.remint(a)
			return
		}
		a.admitted = true
		c.entered(a)
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

func (c *claimant) remint(a *ask) {
	if rel, ok := a.op.Signal().(Release); ok {
		held := false
		for _, hold := range c.fold.Holders(a.op.Key()) {
			if hold.Mint() == rel.Claim {
				held = true
			}
		}
		if !held {
			return
		}
	}
	parent := c.fold.Head(a.op.Key())
	rebuilt := NewOperation(c.minter.Mint(), a.op.Priority(), a.op.Key(), parent, parent+1, a.op.Signal())
	next := &ask{op: rebuilt, acked: make(map[int64]bool), posted: a.posted}
	c.pending[rebuilt.Mint()] = next
	c.mark(next)
	c.publish(rebuilt)
	_ = c.fold.Post(rebuilt)
}

func (c *claimant) left(peer int64) {
	if !c.live[peer] {
		return
	}
	delete(c.live, peer)
	delete(c.peers, peer)
	c.fold.Gone(peer)
	c.releaseFor(peer)
	if c.told != nil {
		c.told(evt.Notice{Source: "room", Text: "member left", Level: evt.LevelInfo})
	}
	for _, a := range c.pending {
		c.mark(a)
	}
	c.reevaluate()
}

func (c *claimant) releaseFor(peer int64) {
	for _, act := range c.fold.Held(peer) {
		parent := c.fold.Head(act.Key())
		op := NewOperation(c.minter.Mint(), act.Priority(), act.Key(), parent, parent+1, Release{Claim: act.Mint()})
		a := &ask{op: op, acked: make(map[int64]bool)}
		c.pending[op.Mint()] = a
		c.mark(a)
		c.publish(op)
		_ = c.fold.Post(op)
	}
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
