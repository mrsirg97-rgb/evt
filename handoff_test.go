package evt_test

import (
	"context"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
)

func engineRunning(t *testing.T) (evt.Engine, context.Context) {
	t.Helper()
	eng := evt.NewEngine()
	ctx, cancel := context.WithCancel(context.Background())
	go eng.Start(ctx)
	t.Cleanup(cancel)
	return eng, ctx
}

func waitReady(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never happened", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestASubmitSaysStartedOrQueued(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 2)
	var first, second, third int
	work1 := evt.Func(func(context.Context) { first++ })
	work2 := evt.Func(func(context.Context) { second++ })
	work3 := evt.Func(func(context.Context) { third++ })
	s1 := cap.Submit(ctx, work1, 30)
	s2 := cap.Submit(ctx, work2, 30)
	s3 := cap.Submit(ctx, work3, 30)
	if !s1.Started() || !s2.Started() {
		t.Fatalf("two places hold two starts: %v %v", s1.Started(), s2.Started())
	}
	if s3.Started() {
		t.Fatal("the third submit must say queued")
	}
	work1.Resolve(ctx)
	work2.Resolve(ctx)
	if first != 1 || second != 1 || third != 0 {
		t.Fatalf("a started submit resolves in the submitter's goroutine, a queued one waits: %d %d %d", first, second, third)
	}
}

func TestTheCapCountsInFlightAndQueued(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 2)
	work1 := evt.Func(func(context.Context) {})
	work2 := evt.Func(func(context.Context) {})
	work3 := evt.Func(func(context.Context) {})
	s1 := cap.Submit(ctx, work1, 30)
	s2 := cap.Submit(ctx, work2, 30)
	s3 := cap.Submit(ctx, work3, 30)
	work1.Resolve(ctx)
	work2.Resolve(ctx)
	waitReady(t, "the queued work counted", func() bool { return cap.Queued() == 1 })
	if cap.InFlight() != 2 || cap.Queued() != 1 {
		t.Fatalf("in flight %d, queued %d", cap.InFlight(), cap.Queued())
	}
	s1.Release()
	<-s3.Settled()
	if cap.InFlight() != 2 || cap.Queued() != 0 {
		t.Fatalf("the completion starts the queued work: %d %d", cap.InFlight(), cap.Queued())
	}
	s2.Release()
	waitReady(t, "the place returning to the cap", func() bool { return cap.InFlight() == 1 })
	if cap.InFlight() != 1 {
		t.Fatalf("the last release frees its place: %d", cap.InFlight())
	}
}

func TestOneCapServesSeveralSubmitters(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 1)
	ranA, ranB := false, false
	workA := evt.Func(func(context.Context) { ranA = true })
	workB := evt.Func(func(context.Context) { ranB = true })
	submitA := func() evt.Submission {
		return cap.Submit(ctx, workA, 30)
	}
	submitB := func() evt.Submission {
		return cap.Submit(ctx, workB, 30)
	}
	sa := submitA()
	sb := submitB()
	if !sa.Started() {
		t.Fatal("the first submitter starts")
	}
	if sb.Started() {
		t.Fatal("the second submitter queues behind the first")
	}
	workA.Resolve(ctx)
	if !ranA {
		t.Fatal("a started submit resolves in the submitter's own goroutine")
	}
	sa.Release()
	<-sb.Settled()
	if !ranB {
		t.Fatal("the second submitter's work runs off the shared cap")
	}
}

func TestAWorkStoppedWhileQueuedSettlesWithItsStop(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 1)
	ranFirst, ranSecond, stopped := false, false, false
	work1 := evt.Func(func(context.Context) { ranFirst = true })
	cap.Submit(ctx, work1, 30)
	work1.Resolve(ctx)
	s2 := cap.Submit(ctx, evt.Func(func(gate context.Context) {
		if gate.Err() != nil {
			stopped = true
			return
		}
		ranSecond = true
	}), 30)
	s2.Stop()
	<-s2.Settled()
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
	if !ranFirst {
		t.Fatal("the started work ran before the stop")
	}
}

func TestNoCapStartsEverything(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 0)
	ran := false
	work := evt.Func(func(context.Context) { ran = true })
	s := cap.Submit(ctx, work, 30)
	if !s.Started() {
		t.Fatal("no cap is no queue")
	}
	work.Resolve(ctx)
	if !ran {
		t.Fatal("a started submit resolves in the submitter's own goroutine")
	}
	if cap.InFlight() != 0 || cap.Queued() != 0 {
		t.Fatalf("no cap holds no counts: %d %d", cap.InFlight(), cap.Queued())
	}
}
