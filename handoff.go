package evt

import (
	"context"
	"sync/atomic"
)

type Cap struct {
	engine  Engine
	workers int
	started atomic.Int64
	queued  atomic.Int64
	pending []*Submission
}

func NewCap(engine Engine, workers int) *Cap {
	return &Cap{engine: engine, workers: workers}
}

func (c *Cap) Size() int {
	if c == nil {
		return 0
	}
	return c.workers
}

func (c *Cap) InFlight() int {
	if c == nil {
		return 0
	}
	return int(c.started.Load())
}

func (c *Cap) Queued() int {
	if c == nil {
		return 0
	}
	return int(c.queued.Load())
}

type Submission struct {
	c        *Cap
	ctx      context.Context
	work     Closure
	priority int
	done     chan struct{}
	place    atomic.Bool
}

func (s *Submission) Started() bool { return s.place.Load() }

func (s *Submission) Settled() <-chan struct{} { return s.done }

func (s *Submission) Stop() {
	if s.place.Load() || s.c == nil || s.c.workers <= 0 {
		return
	}
	s.c.engine.Add(Func(func(consumer context.Context) {
		if s.drop() {
			s.settleStopped()
		}
	}), s.priority)
}

func (s *Submission) Release() {
	if s.c == nil || s.c.workers <= 0 {
		return
	}
	s.c.engine.Add(Func(func(consumer context.Context) {
		s.c.started.Add(-1)
		s.c.startNext()
	}), s.priority)
}

func (c *Cap) Submit(ctx context.Context, work Closure, priority int) *Submission {
	s := &Submission{c: c, ctx: ctx, work: work, priority: priority, done: make(chan struct{})}
	if c == nil || c.workers <= 0 {
		s.place.Store(true)
		close(s.done)
		return s
	}
	for c.queued.Load() == 0 {
		held := c.started.Load()
		if held >= int64(c.workers) {
			break
		}
		if c.started.CompareAndSwap(held, held+1) {
			s.place.Store(true)
			close(s.done)
			return s
		}
	}
	c.engine.Add(Func(func(consumer context.Context) {
		c.pending = append(c.pending, s)
		c.queued.Store(int64(len(c.pending)))
		c.startNext()
	}), priority)
	return s
}

func (c *Cap) startNext() {
	for len(c.pending) > 0 {
		s := c.pending[0]
		if s.ctx.Err() != nil {
			c.shift()
			s.settleStopped()
			continue
		}
		held := c.started.Load()
		if held >= int64(c.workers) {
			return
		}
		if !c.started.CompareAndSwap(held, held+1) {
			continue
		}
		c.shift()
		s.place.Store(true)
		go s.run()
	}
}

func (c *Cap) shift() {
	c.pending = c.pending[1:]
	c.queued.Store(int64(len(c.pending)))
}

func (s *Submission) run() {
	s.work.Resolve(s.ctx)
	close(s.done)
}

func (s *Submission) settleStopped() {
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s.work.Resolve(stopCtx)
	close(s.done)
}

func (s *Submission) drop() bool {
	for i, e := range s.c.pending {
		if e.done == s.done {
			s.c.pending = append(s.c.pending[:i], s.c.pending[i+1:]...)
			s.c.queued.Store(int64(len(s.c.pending)))
			return true
		}
	}
	return false
}
