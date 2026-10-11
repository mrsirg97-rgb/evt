package evt

import (
	"context"
	"sync"
)

type Cap struct {
	mu     sync.Mutex
	places chan struct{}
	queue  []Submission
}

func NewCap(workers int) *Cap {
	if workers <= 0 {
		return &Cap{}
	}
	return &Cap{places: make(chan struct{}, workers)}
}

func (c *Cap) Size() int {
	return cap(c.places)
}

func (c *Cap) InFlight() int {
	return len(c.places)
}

func (c *Cap) Queued() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

type Submission struct {
	c       *Cap
	ctx     context.Context
	work    Closure
	done    chan struct{}
	started bool
}

func (s Submission) Started() bool { return s.started }

func (s Submission) Settled() <-chan struct{} { return s.done }

func (s Submission) Stop() {
	if s.started {
		return
	}
	s.c.settle(s)
}

func (c *Cap) Submit(ctx context.Context, work Closure) Submission {
	s := Submission{c: c, ctx: ctx, work: work, done: make(chan struct{})}
	if c.places == nil {
		s.start(ctx)
		return s
	}
	select {
	case c.places <- struct{}{}:
		s.start(ctx)
	default:
		c.mu.Lock()
		c.queue = append(c.queue, s)
		c.mu.Unlock()
	}
	return s
}

func (s *Submission) start(ctx context.Context) {
	s.started = true
	s.work.Resolve(ctx)
	close(s.done)
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

func (c *Cap) Release() {
	if c.places == nil {
		return
	}
	<-c.places
	c.mu.Lock()
	var live *Submission
	stopped := make([]*Submission, 0, len(c.queue))
	for len(c.queue) > 0 {
		s := c.queue[0]
		c.queue = c.queue[1:]
		if s.ctx.Err() == nil {
			live = &s
			break
		}
		stopped = append(stopped, &s)
	}
	c.mu.Unlock()
	for _, s := range stopped {
		s.settleStopped()
	}
	if live != nil {
		c.places <- struct{}{}
		go live.run()
	}
}

func (c *Cap) settle(s Submission) {
	c.mu.Lock()
	for i, e := range c.queue {
		if e.done == s.done {
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			c.mu.Unlock()
			s.settleStopped()
			return
		}
	}
	c.mu.Unlock()
}
