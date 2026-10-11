package evt_test

import (
	"context"
	"sync"
	"testing"

	"github.com/mrsirg97-rgb/evt"
)

func TestASubmitRacingAReleaseNeverSeatsTwoOnOnePlace(t *testing.T) {
	eng, ctx := engineRunning(t)
	rounds := 200000
	for i := 0; i < rounds; i++ {
		cap := evt.NewCap(eng, 1)
		noop := evt.Func(func(context.Context) {})
		s1 := cap.Submit(ctx, noop, 30)
		s2 := cap.Submit(ctx, noop, 30)
		queued := make(chan struct{})
		eng.Add(evt.Func(func(context.Context) { close(queued) }), 0)
		<-queued
		var s3 *evt.Submission
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s1.Release() }()
		go func() { defer wg.Done(); s3 = cap.Submit(ctx, noop, 30) }()
		wg.Wait()
		drained := make(chan struct{})
		eng.Add(evt.Func(func(context.Context) { close(drained) }), 0)
		<-drained
		if n := cap.InFlight(); n != 1 {
			t.Fatalf("round %d: a cap of one holds %d", i, n)
		}
		if !s2.Started() || s3.Started() {
			t.Fatalf("round %d: the queued work did not take the freed place in submit order (second %v, third %v)", i, s2.Started(), s3.Started())
		}
	}
}
