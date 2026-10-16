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

func TestASubmitOnAStoppedEngineSettlesForTheCaller(t *testing.T) {
	eng := evt.NewEngine()
	ctx, cancel := context.WithCancel(context.Background())
	go eng.Start(ctx)
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for eng.Add(evt.Func(func(context.Context) {}), 30) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the engine never stopped")
		}
		time.Sleep(time.Millisecond)
	}
	cap := evt.NewCap(eng, 1)
	s := cap.Submit(context.Background(), evt.Func(func(context.Context) {}), 30)
	if !s.Started() {
		t.Fatal("on a stopped engine the caller takes the place itself")
	}
	select {
	case <-s.Settled():
	case <-time.After(time.Second):
		t.Fatal("a submit on a stopped engine must settle, not hang the caller on <-Settled()")
	}
	s.Release()
	if got := cap.InFlight(); got != 0 {
		t.Fatalf("the release returns the place it took: InFlight %d, want 0", got)
	}
}

func TestAReleaseOfAnUnstartedSubmissionIsANoOp(t *testing.T) {
	eng, ctx := engineRunning(t)
	cap := evt.NewCap(eng, 1)
	s1 := cap.Submit(ctx, evt.Func(func(context.Context) {}), 30)
	queuedCtx, stop := context.WithCancel(ctx)
	s2 := cap.Submit(queuedCtx, evt.Func(func(context.Context) {}), 30)
	if s2.Started() {
		t.Fatal("the second submit must say queued")
	}
	stop()
	waitReady(t, "the queued submission settles stopped", func() bool {
		select {
		case <-s2.Settled():
			return true
		default:
			return false
		}
	})
	s2.Release()
	if got := cap.InFlight(); got != 1 {
		s1.Release()
		t.Fatalf("a release of a submission that never took a place erodes the cap: InFlight %d, want 1", got)
	}
	s1.Release()
}

func TestASubmissionQueuedBeforeAStopSettlesOnRelease(t *testing.T) {
	eng := evt.NewEngine()
	ctx, cancel := context.WithCancel(context.Background())
	go eng.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for eng.Add(evt.Func(func(context.Context) {}), 30) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the engine never began")
		}
		time.Sleep(time.Millisecond)
	}
	cap := evt.NewCap(eng, 1)
	a := cap.Submit(ctx, evt.Func(func(context.Context) {}), 30)
	if !a.Started() {
		t.Fatal("the first place is the caller's")
	}
	live, stopLive := context.WithCancel(context.Background())
	t.Cleanup(stopLive)
	b := cap.Submit(live, evt.Func(func(context.Context) {}), 30)
	if b.Started() {
		t.Fatal("the second submit must say queued")
	}
	cancel()
	deadline = time.Now().Add(5 * time.Second)
	for eng.Add(evt.Func(func(context.Context) {}), 30) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the engine never stopped")
		}
		time.Sleep(time.Millisecond)
	}
	a.Release()
	b.Stop()
	select {
	case <-b.Settled():
	case <-time.After(2 * time.Second):
		t.Fatal("a submission queued before the stop must settle when its place is released or it is stopped")
	}
}
