package evt_test

import (
	"context"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
)

func TestASubmitSaysStartedOrQueued(t *testing.T) {
	cap := evt.NewCap(2)
	var first, second, third int
	s1 := cap.Submit(context.Background(), evt.Func(func(context.Context) { first++ }))
	s2 := cap.Submit(context.Background(), evt.Func(func(context.Context) { second++ }))
	s3 := cap.Submit(context.Background(), evt.Func(func(context.Context) { third++ }))
	if !s1.Started() || !s2.Started() {
		t.Fatalf("two places hold two starts: %v %v", s1.Started(), s2.Started())
	}
	if s3.Started() {
		t.Fatal("the third submit must say queued")
	}
	if first != 1 || second != 1 || third != 0 {
		t.Fatalf("a started submit resolves here, a queued one waits: %d %d %d", first, second, third)
	}
}

func TestTheCapCountsInFlightAndQueued(t *testing.T) {
	cap := evt.NewCap(2)
	cap.Submit(context.Background(), evt.Func(func(context.Context) {}))
	cap.Submit(context.Background(), evt.Func(func(context.Context) {}))
	s3 := cap.Submit(context.Background(), evt.Func(func(context.Context) {}))
	if cap.InFlight() != 2 || cap.Queued() != 1 {
		t.Fatalf("in flight %d, queued %d", cap.InFlight(), cap.Queued())
	}
	cap.Release()
	if cap.InFlight() != 2 || cap.Queued() != 0 {
		t.Fatalf("the place transfers to the queued work: %d %d", cap.InFlight(), cap.Queued())
	}
	<-s3.Settled()
	cap.Release()
	if cap.InFlight() != 1 {
		t.Fatalf("the last release frees its place: %d", cap.InFlight())
	}
}

func TestOneCapServesSeveralSubmitters(t *testing.T) {
	cap := evt.NewCap(1)
	ranA, ranB := false, false
	submitA := func() evt.Submission {
		return cap.Submit(context.Background(), evt.Func(func(context.Context) { ranA = true }))
	}
	submitB := func() evt.Submission {
		return cap.Submit(context.Background(), evt.Func(func(context.Context) { ranB = true }))
	}
	sa := submitA()
	sb := submitB()
	if !sa.Started() {
		t.Fatal("the first submitter starts")
	}
	if sb.Started() {
		t.Fatal("the second submitter queues behind the first")
	}
	if !ranA {
		t.Fatal("a started submit resolves in the submitter's own goroutine")
	}
	cap.Release()
	<-sb.Settled()
	if !ranB {
		t.Fatal("the second submitter's work runs off the shared cap")
	}
}

func TestAWorkStoppedWhileQueuedSettlesWithItsStop(t *testing.T) {
	cap := evt.NewCap(1)
	ranFirst, ranSecond, stopped := false, false, false
	cap.Submit(context.Background(), evt.Func(func(context.Context) { ranFirst = true }))
	s2 := cap.Submit(context.Background(), evt.Func(func(ctx context.Context) {
		if ctx.Err() != nil {
			stopped = true
			return
		}
		ranSecond = true
	}))
	s2.Stop()
	if !stopped {
		t.Fatal("the work settles with its stop")
	}
	if ranSecond {
		t.Fatal("a work stopped while queued never starts")
	}
	if cap.Queued() != 0 {
		t.Fatalf("the queue dropped it: %d", cap.Queued())
	}
	if cap.InFlight() != 1 {
		t.Fatalf("the stop takes no place: %d", cap.InFlight())
	}
	select {
	case <-s2.Settled():
	case <-time.After(time.Second):
		t.Fatal("the settlement is observable on the submission")
	}
	if !ranFirst {
		t.Fatal("the started work ran before the stop")
	}
}
