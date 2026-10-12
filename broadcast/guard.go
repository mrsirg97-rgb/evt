package broadcast

import (
	"sync/atomic"
)

type Guard interface {
	Admit(op Operation) bool
	Version(key int64) int64
	Rewind(key, version int64)
	Snapshot() map[int64]int64
}

type guard struct {
	heads map[int64]*atomic.Int64
}

func NewGuard() Guard {
	return &guard{heads: make(map[int64]*atomic.Int64)}
}

func NewGuardFrom(heads map[int64]int64) Guard {
	g := &guard{heads: make(map[int64]*atomic.Int64, len(heads))}
	for key, version := range heads {
		h := &atomic.Int64{}
		h.Store(version)
		g.heads[key] = h
	}
	return g
}

func (g *guard) head(key int64) *atomic.Int64 {
	h, ok := g.heads[key]
	if !ok {
		h = &atomic.Int64{}
		g.heads[key] = h
	}
	return h
}

func (g *guard) Admit(op Operation) bool {
	return g.head(op.Key()).CompareAndSwap(op.Parent(), op.Version())
}

func (g *guard) Version(key int64) int64 {
	return g.head(key).Load()
}

func (g *guard) Rewind(key, version int64) {
	g.head(key).Store(version)
}

func (g *guard) Snapshot() map[int64]int64 {
	out := make(map[int64]int64, len(g.heads))
	for key, h := range g.heads {
		out[key] = h.Load()
	}
	return out
}
