package broadcast

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/mrsirg97-rgb/evt"
)

func MemberOf(identity []byte) int64 {
	sum := sha256.Sum256(identity)
	return int64(binary.BigEndian.Uint64(sum[:8]) &^ (1 << 63))
}

type machine struct {
	wall  uint64
	epoch time.Time
}

func MachineClock() evt.Clock {
	now := time.Now()
	return &machine{wall: uint64(now.UnixNano()), epoch: now}
}

func (m *machine) Step() uint64 {
	return m.wall + uint64(time.Since(m.epoch).Nanoseconds())
}

type Mint struct {
	Step   uint64
	Member int64
	Nonce  int64
}

type Minter interface {
	Mint() Mint
	Member() int64
}

type minter struct {
	member int64
	clock  evt.Clock
	step   uint64
	nonce  int64
}

func NewMinter(member int64, clock evt.Clock) Minter {
	return &minter{member: member, clock: clock}
}

func (m *minter) Mint() Mint {
	step := m.clock.Step()
	if step > m.step {
		m.step = step
		m.nonce = 0
	}
	mint := Mint{Step: m.step, Member: m.member, Nonce: m.nonce}
	m.nonce++
	return mint
}

func (m *minter) Member() int64 { return m.member }
