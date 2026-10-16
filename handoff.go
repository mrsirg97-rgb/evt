package evt

import (
	"context"
	"sync/atomic"
)

type Cap interface {
	Size() int
	InFlight() int
	Queued() int
	Submit(ctx context.Context, work Closure, priority int) Submission
}

type cap struct {
	engine  Engine
	workers int
	started atomic.Int64
	queued  atomic.Int64
	// pending is touched only by the consumer: every read and write
	// happens inside an event on c.engine, which runs one at a time.
	pending []*submission
}

func NewCap(engine Engine, workers int) Cap {
	return &cap{engine: engine, workers: workers}
}

func (c *cap) Size() int {
	return c.workers
}

func (c *cap) InFlight() int {
	return int(c.started.Load())
}

func (c *cap) Queued() int {
	return int(c.queued.Load())
}

type Submission interface {
	Started() bool
	Settled() <-chan struct{}
	Stop()
	Release()
}

type submission struct {
	c        *cap
	ctx      context.Context
	work     Closure
	priority int
	done     chan struct{}
	place    atomic.Bool
}

func (s *submission) Started() bool { return s.place.Load() }

func (s *submission) Settled() <-chan struct{} { return s.done }

func (s *submission) Stop() {
	if s.place.Load() || s.c.workers <= 0 {
		return
	}
	if s.c.engine.Add(Func(func(consumer context.Context) {
		if s.drop() {
			s.settleStopped()
		}
	}), s.priority) == 0 {
		// a stopped engine drops the add: the queue never formed, so the
		// submission is already settled by Submit's stopped arm.
		return
	}
}

func (s *submission) Release() {
	if s.c.workers <= 0 || !s.place.Load() {
		return
	}
	if s.c.engine.Add(Func(func(consumer context.Context) {
		s.c.started.Add(-1)
		s.c.startNext()
	}), s.priority) == 0 {
		s.c.started.Add(-1)
	}
}

func (c *cap) Submit(ctx context.Context, work Closure, priority int) Submission {
	s := &submission{c: c, ctx: ctx, work: work, priority: priority, done: make(chan struct{})}
	if c.workers <= 0 {
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
	if c.engine.Add(Func(func(consumer context.Context) {
		c.pending = append(c.pending, s)
		c.queued.Store(int64(len(c.pending)))
		c.startNext()
	}), priority) == 0 {
		// a stopped engine drops adds: the queue would never drain, so the
		// caller takes the place itself and settles, as on the fast path.
		c.started.Add(1)
		s.place.Store(true)
		close(s.done)
	}
	return s
}

func (c *cap) startNext() {
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

func (c *cap) shift() {
	c.pending = c.pending[1:]
	c.queued.Store(int64(len(c.pending)))
}

func (s *submission) run() {
	s.work.Resolve(s.ctx)
	close(s.done)
}

func (s *submission) settleStopped() {
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s.work.Resolve(stopCtx)
	close(s.done)
}

func (s *submission) drop() bool {
	for i, e := range s.c.pending {
		if e.done == s.done {
			s.c.pending = append(s.c.pending[:i], s.c.pending[i+1:]...)
			s.c.queued.Store(int64(len(s.c.pending)))
			return true
		}
	}
	return false
}
