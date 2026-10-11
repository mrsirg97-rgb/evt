package evt_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/mrsirg97-rgb/evt"
)

func TestAReleaseRacingASubmitNeverLeavesWorkQueuedBehindAFreePlace(t *testing.T) {
	eng, ctx := engineRunning(t)
	rounds := 300000
	for i := 0; i < rounds; i++ {
		cap := evt.NewCap(eng, 1)
		var ranSecond atomic.Bool
		work1 := evt.Func(func(context.Context) {})
		s1 := cap.Submit(ctx, work1, 30)
		if !s1.Started() {
			t.Fatalf("round %d: the first submit starts", i)
		}
		work1.Resolve(ctx)
		work2 := evt.Func(func(context.Context) { ranSecond.Store(true) })
		done := make(chan struct{})
		go s1.Release()
		go func() {
			s2 := cap.Submit(ctx, work2, 30)
			if s2.Started() {
				work2.Resolve(ctx)
			}
			<-s2.Settled()
			close(done)
		}()
		<-done
		if cap.Queued() != 0 {
			t.Fatalf("round %d: work left queued behind a free place: %d", i, cap.Queued())
		}
		if cap.InFlight() != 1 {
			t.Fatalf("round %d: the place did not transfer: %d", i, cap.InFlight())
		}
	}
}
