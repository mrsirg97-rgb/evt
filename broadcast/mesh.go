package broadcast

import (
	"context"
	"sort"
	"time"

	"github.com/mrsirg97-rgb/evt"
)

type MeshConfig interface {
	Machine() string
	Addr() string
	Peers() []string
	Clock() MeshClock
	Timeout() time.Duration
	Idle() time.Duration
	Window() int64
	Rounds() int
	Voice() int
	HasTold() bool
	Told(act Act)
}

type MeshOption func(cfg *meshConfig)

type meshConfig struct {
	machine string
	addr    string
	peers   []string
	clock   MeshClock
	timeout time.Duration
	idle    time.Duration
	window  int64
	rounds  int
	voice   int
	told    func(Act)
}

func NewMeshConfig(machine, addr string, clock MeshClock, opts ...MeshOption) MeshConfig {
	cfg := &meshConfig{machine: machine, addr: addr, clock: clock}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func WithPeers(peers ...string) MeshOption {
	return func(cfg *meshConfig) { cfg.peers = peers }
}

func WithTimeout(timeout time.Duration) MeshOption {
	return func(cfg *meshConfig) { cfg.timeout = timeout }
}

func WithIdle(idle time.Duration) MeshOption {
	return func(cfg *meshConfig) { cfg.idle = idle }
}

func WithWindow(window int64) MeshOption {
	return func(cfg *meshConfig) { cfg.window = window }
}

func WithRounds(rounds int) MeshOption {
	return func(cfg *meshConfig) { cfg.rounds = rounds }
}

func WithVoice(voice int) MeshOption {
	return func(cfg *meshConfig) { cfg.voice = voice }
}

func WithTold(told func(Act)) MeshOption {
	return func(cfg *meshConfig) { cfg.told = told }
}

func (cfg *meshConfig) Machine() string        { return cfg.machine }
func (cfg *meshConfig) Addr() string           { return cfg.addr }
func (cfg *meshConfig) Peers() []string        { return cfg.peers }
func (cfg *meshConfig) Clock() MeshClock       { return cfg.clock }
func (cfg *meshConfig) Timeout() time.Duration { return cfg.timeout }
func (cfg *meshConfig) Idle() time.Duration    { return cfg.idle }
func (cfg *meshConfig) Window() int64          { return cfg.window }
func (cfg *meshConfig) Rounds() int            { return cfg.rounds }
func (cfg *meshConfig) Voice() int             { return cfg.voice }
func (cfg *meshConfig) HasTold() bool          { return cfg.told != nil }
func (cfg *meshConfig) Told(act Act)           { cfg.told(act) }

type Mesh interface {
	MeshConfig
	Minter
	Id() int64
	Start(ctx context.Context) error
	Stop()
	Post(key int64, priority int, claim Mint) (Act, bool)
	Release(claim Act) bool
	Holders(key int64) []Act
	Machines() []string
	Horizon() int64
	Order() []Act
	Reorgs() int
	Refused() int
}

type mesh struct {
	id        int64
	cfg       MeshConfig
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
	if cfg.Clock() == nil {
		panic("broadcast: a mesh needs its clock")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &mesh{
		id:        id,
		cfg:       cfg,
		transport: NewNetTransport(id, cfg.Addr(), cfg.Peers(), cfg.Timeout(), cfg.Idle(), MeshCodec()),
		seq:       NewSequencer(cfg.Window()),
		state:     NewMeshState().(*meshState),
		wal:       NewWAL(),
		minter:    NewMinter(id, cfg.Clock()),
		engine:    evt.NewEngine(),
		ctx:       ctx,
		cancel:    cancel,
		missed:    map[int64]int{},
		addrID:    map[string]int64{},
		idMachine: map[int64]string{},
		seen:      map[string]bool{cfg.Machine(): true},
	}
	return m
}

func (m *mesh) Machine() string        { return m.cfg.Machine() }
func (m *mesh) Addr() string           { return m.cfg.Addr() }
func (m *mesh) Peers() []string        { return m.cfg.Peers() }
func (m *mesh) Clock() MeshClock       { return m.cfg.Clock() }
func (m *mesh) Timeout() time.Duration { return m.cfg.Timeout() }
func (m *mesh) Idle() time.Duration    { return m.cfg.Idle() }
func (m *mesh) Window() int64          { return m.cfg.Window() }
func (m *mesh) Rounds() int            { return m.cfg.Rounds() }
func (m *mesh) Voice() int             { return m.cfg.Voice() }
func (m *mesh) HasTold() bool          { return m.cfg.HasTold() }
func (m *mesh) Told(act Act)           { m.cfg.Told(act) }

func (m *mesh) Mint() Mint {
	var mint Mint
	if !m.ask(func() { mint = m.minter.Mint() }) {
		return Mint{}
	}
	return mint
}

func (m *mesh) Member() int64 { return m.minter.Member() }
func (m *mesh) Id() int64     { return m.id }

func (m *mesh) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		m.Stop()
	}()
	go m.engine.Start(m.ctx)
	go m.transport.Recv(m.ctx, m.receive)
	m.Clock().Start(m.ctx, m.engine, m.onTick)
	return m.transport.Serve(m.ctx, nil, m.sync, m.serveGossip)
}

func (m *mesh) Stop() {
	m.cancel()
	m.transport.Close()
}

func (m *mesh) Post(key int64, priority int, claim Mint) (Act, bool) {
	done := make(chan Act, 1)
	m.engine.Add(evt.Func(func(context.Context) {
		claimed := NewAct(claim, key, priority, m.Machine(), "", nil, nil, nil)
		act := NewAct(m.minter.Mint(), key, priority, m.Machine(), ActClaim, claimed, m.Clock().Output(), m.Clock().Proof())
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
		act := NewAct(m.minter.Mint(), claim.Key(), claim.Priority(), m.Machine(), ActRelease, claim, m.Clock().Output(), m.Clock().Proof())
		if !m.absorb(act) {
			close(done)
			return
		}
		done <- act
	}), claim.Priority())
	_, ok := m.wait(done)
	return ok
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

func (m *mesh) Horizon() int64 {
	var horizon int64
	if !m.ask(func() { horizon = m.seq.Horizon() }) {
		return 0
	}
	return horizon
}

func (m *mesh) Order() []Act {
	var out []Act
	if !m.ask(func() { out = m.seq.Order() }) {
		return nil
	}
	return out
}

func (m *mesh) Reorgs() int {
	var reorgs int
	if !m.ask(func() { reorgs = m.seq.Reorgs() }) {
		return 0
	}
	return reorgs
}

func (m *mesh) Refused() int {
	var refused int
	if !m.ask(func() { refused = m.seq.Refused() }) {
		return 0
	}
	return refused
}

func (m *mesh) absorb(act Act) bool {
	if act.Mint().Member == m.Id() && act.Machine() != m.Machine() {
		return false
	}
	if !m.Clock().Record(act.Tick(), act.Output(), act.Proof()) {
		return false
	}
	if err := m.seq.Apply(m.state, act); err != nil {
		return false
	}
	m.wal.Append(act)
	m.idMachine[act.Mint().Member] = act.Machine()
	m.seen[act.Machine()] = true
	m.deliver(act)
	return true
}

func (m *mesh) ask(f func()) bool {
	done := make(chan struct{})
	m.engine.Add(evt.Func(func(context.Context) { f(); close(done) }), m.Voice())
	select {
	case <-done:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *mesh) deliver(act Act) {
	if act.Machine() == m.Machine() {
		return
	}
	if int64(act.Tick()) < m.seq.Horizon()-m.Window() {
		if m.HasTold() {
			m.Told(act)
		}
		return
	}
	m.provisional = append(m.provisional, act)
}

func (m *mesh) expire(id int64) {
	machine, ok := m.idMachine[id]
	if !ok {
		return
	}
	delete(m.seen, machine)
	for _, claim := range m.state.holdsOf(machine) {
		act := NewAct(m.minter.Mint(), claim.Key(), claim.Priority(), m.Machine(), ActRelease, claim, m.Clock().Output(), m.Clock().Proof())
		m.absorb(act)
		m.fanout(act)
	}
}

func (m *mesh) fanout(act Act) {
	go m.transport.Send(m.ctx, func(error) {}, NewMessage(m.Id(), true, act))
}

func (m *mesh) hit(id int64) {
	m.missed[id] = 0
}

func (m *mesh) learn(peer string, message Message) {
	if id := message.Origin(); id != 0 && id != m.Id() {
		m.addrID[peer] = id
		m.hit(id)
	}
}

func (m *mesh) live() []string {
	out := make([]string, 0, len(m.seen))
	for machine := range m.seen {
		out = append(out, machine)
	}
	sort.Strings(out)
	return out
}

func (m *mesh) observe(index uint64) {
	m.seq.Observe(int64(index))
	m.wal.Compact(m.seq.Horizon() - m.Window())
	m.recheck()
}

func (m *mesh) onTick(index uint64) {
	m.observe(index)
	m.round()
}

func (m *mesh) receive(err error, messages ...Message) {
	if err != nil {
		return
	}
	for _, message := range messages {
		m.engine.Add(evt.Func(func(context.Context) { m.take(message) }), m.Voice())
	}
}

func (m *mesh) recheck() {
	kept := m.provisional[:0]
	for _, act := range m.provisional {
		if int64(act.Tick()) < m.seq.Horizon()-m.Window() {
			if m.HasTold() {
				m.Told(act)
			}
			continue
		}
		kept = append(kept, act)
	}
	m.provisional = kept
}

func (m *mesh) round() {
	summary := Summarize(m.wal.Mints()).hash
	for _, peer := range m.Peers() {
		go func(peer string) {
			missing, err := m.transport.Gossip(m.ctx, peer, summary)
			m.engine.Add(evt.Func(func(context.Context) {
				if err != nil {
					if id, ok := m.addrID[peer]; ok {
						m.missed[id]++
						if m.missed[id] >= m.Rounds() {
							m.expire(id)
						}
					}
					return
				}
				for _, message := range missing {
					m.learn(peer, message)
					m.take(message)
				}
			}), m.Voice())
		}(peer)
	}
}

func (m *mesh) serveGossip(theirSummary map[int]uint64) []Message {
	var out []Message
	if !m.ask(func() {
		out = append(out, NewMessage(m.Id(), true))
		for _, tick := range m.Clock().Entries() {
			out = append(out, NewMessage(m.Id(), true, tick))
		}
		mine := Summarize(m.wal.Mints())
		for b, myHash := range mine.hash {
			if theirSummary[b] == myHash {
				continue
			}
			for _, mint := range mine.keys[b] {
				if act, ok := m.wal.Act(mint); ok {
					out = append(out, NewMessage(m.Id(), true, act))
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

func (m *mesh) stamp(tick Tick) {
	if tick.Index <= m.Clock().Step() {
		return
	}
	if !m.Clock().Record(tick.Index, tick.Output, tick.Proof) && !m.Clock().Trust(tick.Index, tick.Output, tick.Proof) {
		return
	}
	m.observe(m.Clock().Step())
}

func (m *mesh) sync(since int64) []Message {
	var out []Message
	if !m.ask(func() {
		out = append(out, NewMessage(m.Id(), true))
		for _, tick := range m.Clock().Entries() {
			out = append(out, NewMessage(m.Id(), true, tick))
		}
		for _, act := range m.wal.Replay(since) {
			out = append(out, NewMessage(m.Id(), true, act))
		}
	}) {
		return nil
	}
	return out
}

func (m *mesh) take(message Message) {
	switch signal := message.Signal().(type) {
	case Act:
		m.absorb(signal)
	case Tick:
		m.stamp(signal)
	}
}

func (m *mesh) wait(done chan Act) (Act, bool) {
	select {
	case act, ok := <-done:
		return act, ok
	case <-m.ctx.Done():
		return nil, false
	}
}
