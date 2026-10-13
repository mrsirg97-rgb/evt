package broadcast

import (
	"context"
	"math/big"
	"sort"
	"time"

	"github.com/mrsirg97-rgb/evt"
)

type MeshConfig struct {
	Room       string
	Machine    string
	Addr       string
	Peers      []string
	Prime      *big.Int
	Iterations int64
	Timeout    time.Duration
	Idle       time.Duration
	Window     int64
	Rounds     int
	Voice      int
	Told       func(Act)
}

type Mesh interface {
	Start(ctx context.Context) error
	Stop()
	Post(key int64, priority int) (Act, bool)
	Release(claim Act) bool
	Holders(key int64) []Act
	Machines() []string
	Horizon() int64
}

type mesh struct {
	id        int64
	cfg       MeshConfig
	clock     VDFClock
	transport NetTransport
	seq       Sequencer
	state     *meshState
	wal       WAL
	minter    Minter
	engine    evt.Engine

	ctx         context.Context
	cancel      context.CancelFunc
	missed      map[int64]int
	addrID      map[string]int64
	idMachine   map[int64]string
	seen        map[string]bool
	provisional []Act
}

func NewMesh(id int64, cfg MeshConfig) Mesh {
	seed := GenesisSeed(cfg.Room)
	clock := NewVDFClock(cfg.Prime, cfg.Iterations, seed, cfg.Window, cfg.Voice)
	ctx, cancel := context.WithCancel(context.Background())
	m := &mesh{
		id:        id,
		cfg:       cfg,
		clock:     clock,
		transport: NewNetTransport(id, cfg.Addr, cfg.Peers, cfg.Timeout, cfg.Idle, MeshCodec()),
		seq:       NewSequencer(cfg.Window),
		state:     NewMeshState().(*meshState),
		wal:       NewWAL(),
		minter:    NewMinter(id, clock),
		engine:    evt.NewEngine(),
		ctx:       ctx,
		cancel:    cancel,
		missed:    map[int64]int{},
		addrID:    map[string]int64{},
		idMachine: map[int64]string{},
		seen:      map[string]bool{cfg.Machine: true},
	}
	return m
}

func (m *mesh) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		m.Stop()
	}()
	go m.engine.Start(m.ctx)
	go m.transport.Recv(m.ctx, m.receive)
	m.clock.Start(m.ctx, m.engine, m.onTick)
	return m.transport.Serve(m.ctx, nil, m.sync, m.serveGossip)
}

func (m *mesh) Stop() {
	m.cancel()
	m.transport.Close()
}

func (m *mesh) Post(key int64, priority int) (Act, bool) {
	done := make(chan Act, 1)
	m.engine.Add(evt.Func(func(context.Context) {
		act := m.mintAct(key, priority, ActClaim, nil)
		if !m.absorb(act) {
			close(done)
			return
		}
		done <- act
	}), priority)
	return m.wait(done)
}

func (m *mesh) Release(claim Act) bool {
	done := make(chan Act, 1)
	m.engine.Add(evt.Func(func(context.Context) {
		act := m.mintAct(claim.Key, claim.Priority, ActRelease, &claim)
		if !m.absorb(act) {
			close(done)
			return
		}
		done <- act
	}), claim.Priority)
	_, ok := m.wait(done)
	return ok
}

func (m *mesh) wait(done chan Act) (Act, bool) {
	select {
	case act, ok := <-done:
		return act, ok
	case <-m.ctx.Done():
		return Act{}, false
	}
}

func (m *mesh) mintAct(key int64, priority int, kind ActKind, claim *Act) Act {
	return Act{
		Mint:     m.minter.Mint(),
		Key:      key,
		Priority: priority,
		Machine:  m.cfg.Machine,
		Kind:     kind,
		Claim:    claim,
		Output:   m.clock.Output(),
		Proof:    m.clock.Proof(),
	}
}

func (m *mesh) absorb(act Act) bool {
	if !m.clock.Record(act.Tick(), act.Output, act.Proof) {
		return false
	}
	if err := m.seq.Apply(m.state, act); err != nil {
		return false
	}
	m.wal.Append(act)
	m.idMachine[act.Mint.Member] = act.Machine
	m.seen[act.Machine] = true
	m.deliver(act)
	return true
}

func (m *mesh) deliver(act Act) {
	if act.Machine == m.cfg.Machine {
		return
	}
	if int64(act.Tick()) < m.seq.Horizon()-m.cfg.Window {
		if m.cfg.Told != nil {
			m.cfg.Told(act)
		}
		return
	}
	m.provisional = append(m.provisional, act)
}

func (m *mesh) onTick(index uint64) {
	m.observe(index)
	m.round()
}

func (m *mesh) observe(index uint64) {
	m.seq.Observe(int64(index))
	m.wal.Compact(m.seq.Horizon() - m.cfg.Window)
	m.recheck()
}

func (m *mesh) recheck() {
	kept := m.provisional[:0]
	for _, act := range m.provisional {
		if int64(act.Tick()) < m.seq.Horizon()-m.cfg.Window {
			if m.cfg.Told != nil {
				m.cfg.Told(act)
			}
			continue
		}
		kept = append(kept, act)
	}
	m.provisional = kept
}

func (m *mesh) round() {
	summary := Summarize(m.wal.Mints()).hash
	for _, peer := range m.cfg.Peers {
		go func(peer string) {
			missing, err := m.transport.Gossip(m.ctx, peer, summary)
			m.engine.Add(evt.Func(func(context.Context) {
				if err != nil {
					if id, ok := m.addrID[peer]; ok {
						m.missed[id]++
						if m.missed[id] >= m.cfg.Rounds {
							m.expire(id)
						}
					}
					return
				}
				for _, message := range missing {
					m.learn(peer, message)
					m.take(message)
				}
			}), m.cfg.Voice)
		}(peer)
	}
}

func (m *mesh) learn(peer string, message Message) {
	if id := message.Origin(); id != 0 {
		m.addrID[peer] = id
		m.hit(id)
	}
}

func (m *mesh) hit(id int64) {
	m.missed[id] = 0
}

func (m *mesh) expire(id int64) {
	machine, ok := m.idMachine[id]
	if !ok {
		return
	}
	delete(m.seen, machine)
	for _, claim := range m.state.holdsOf(machine) {
		act := m.mintAct(claim.Key, claim.Priority, ActRelease, &claim)
		m.absorb(act)
		m.fanout(act)
	}
}

func (m *mesh) receive(err error, messages ...Message) {
	if err != nil {
		return
	}
	for _, message := range messages {
		m.engine.Add(evt.Func(func(context.Context) { m.take(message) }), m.cfg.Voice)
	}
}

func (m *mesh) take(message Message) {
	switch signal := message.Signal().(type) {
	case Act:
		m.absorb(signal)
	case Tick:
		m.stamp(signal)
	}
}

func (m *mesh) stamp(tick Tick) {
	if tick.Index <= m.clock.Step() {
		return
	}
	if !m.clock.Record(tick.Index, tick.Output, tick.Proof) && !m.clock.Trust(tick.Index, tick.Output, tick.Proof) {
		return
	}
	m.observe(m.clock.Step())
}

func (m *mesh) fanout(act Act) {
	go m.transport.Send(m.ctx, func(error) {}, NewMessage(m.id, true, act))
}

func (m *mesh) sync(since int64) []Message {
	var out []Message
	if !m.ask(func() {
		out = append(out, NewMessage(m.id, true))
		for _, tick := range m.clock.Entries() {
			out = append(out, NewMessage(m.id, true, tick))
		}
		for _, act := range m.wal.Replay(since) {
			out = append(out, NewMessage(m.id, true, act))
		}
	}) {
		return nil
	}
	return out
}

func (m *mesh) serveGossip(theirSummary map[int]uint64) []Message {
	var out []Message
	if !m.ask(func() {
		out = append(out, NewMessage(m.id, true))
		for _, tick := range m.clock.Entries() {
			out = append(out, NewMessage(m.id, true, tick))
		}
		mine := Summarize(m.wal.Mints())
		for b, myHash := range mine.hash {
			if theirSummary[b] == myHash {
				continue
			}
			for _, mint := range mine.keys[b] {
				if act, ok := m.wal.Act(mint); ok {
					out = append(out, NewMessage(m.id, true, act))
					if len(out) >= gossipChunk {
						return
					}
				}
			}
		}
	}) {
		return nil
	}
	return out
}

func (m *mesh) Holders(key int64) []Act {
	var out []Act
	if !m.ask(func() { out = m.state.holders(key) }) {
		return nil
	}
	return out
}

func (m *mesh) Machines() []string {
	var out []string
	if !m.ask(func() { out = m.live() }) {
		return nil
	}
	return out
}

func (m *mesh) live() []string {
	out := make([]string, 0, len(m.seen))
	for machine := range m.seen {
		out = append(out, machine)
	}
	sort.Strings(out)
	return out
}

func (m *mesh) Horizon() int64 {
	var horizon int64
	if !m.ask(func() { horizon = m.seq.Horizon() }) {
		return 0
	}
	return horizon
}

func (m *mesh) ask(f func()) bool {
	done := make(chan struct{})
	m.engine.Add(evt.Func(func(context.Context) { f(); close(done) }), m.cfg.Voice)
	select {
	case <-done:
		return true
	case <-m.ctx.Done():
		return false
	}
}
