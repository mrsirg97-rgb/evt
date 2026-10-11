package broadcast_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

const prioRoom = 30

type inbox struct {
	mu   sync.Mutex
	got  [][]broadcast.Message
	errs []error
	seen chan struct{}
}

func newInbox() *inbox { return &inbox{seen: make(chan struct{}, 64)} }

func (b *inbox) take(err error, messages ...broadcast.Message) {
	b.mu.Lock()
	if err != nil {
		b.errs = append(b.errs, err)
	} else {
		b.got = append(b.got, messages)
	}
	b.mu.Unlock()
	b.seen <- struct{}{}
}

func (b *inbox) batches() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

func (b *inbox) await(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for b.batches() < n {
		select {
		case <-b.seen:
		case <-deadline:
			t.Fatalf("waited for %d batches, have %d", n, b.batches())
		}
	}
}

type workers struct {
	Pending int
	Names   []string

	evt.Mark
}

func (workers) Snapshot() {}

type testEncoder struct{}

type frame struct {
	Origin int64       `json:"origin"`
	Ok     bool        `json:"ok"`
	Notice *evt.Notice `json:"notice,omitempty"`
	Status *workers    `json:"status,omitempty"`
}

func (testEncoder) Encode(m broadcast.Message) ([]byte, error) {
	f := frame{Origin: m.Origin(), Ok: m.Ok()}
	switch ev := m.Signal().(type) {
	case nil:
	case evt.Notice:
		f.Notice = &ev
	case workers:
		f.Status = &ev
	default:
		return nil, fmt.Errorf("test: %T does not cross", ev)
	}
	return json.Marshal(f)
}

func (testEncoder) Decode(encoded []byte) (broadcast.Message, error) {
	var f frame
	if err := json.Unmarshal(encoded, &f); err != nil {
		return nil, fmt.Errorf("test: %v", err)
	}
	switch {
	case f.Notice != nil:
		return broadcast.NewMessage(f.Origin, f.Ok, *f.Notice), nil
	case f.Status != nil:
		return broadcast.NewMessage(f.Origin, f.Ok, *f.Status), nil
	}
	return broadcast.NewMessage(f.Origin, f.Ok), nil
}

func loopRoom(t *testing.T) (broadcast.Room, evt.Engine) {
	t.Helper()
	engine := evt.NewEngine()
	room := broadcast.NewRoom("session", func(origin int64) broadcast.Transport {
		return broadcast.NewLoopTransport(origin, engine, prioRoom)
	})
	return room, engine
}

func TestABroadcastReachesEveryOtherMemberOnceOnTheLoop(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b, c := room.Add(1), room.Add(2), room.Add(3)
	ia, ib, ic := newInbox(), newInbox(), newInbox()
	a.Subscribe(ctx, ia.take)
	b.Subscribe(ctx, ib.take)
	c.Subscribe(ctx, ic.take)
	var ack error
	a.Publish(ctx, func(e error) { ack = e }, broadcast.NewMessage(1, true, evt.Notice{Source: "swarm", Text: "claimed t3"}))
	if ack != nil {
		t.Fatalf("a post is the ack, got %v", ack)
	}
	if n := len(engine.Pending()); n != 2 {
		t.Fatalf("two deliveries wait in the queue before the loop runs, got %d", n)
	}
	go engine.Start(ctx)
	defer engine.Stop()
	ib.await(t, 1)
	ic.await(t, 1)
	for name, in := range map[string]*inbox{"b": ib, "c": ic} {
		msgs := in.got[0]
		if len(msgs) != 1 || msgs[0].Origin() != 1 || msgs[0].Signal().(evt.Notice).Text != "claimed t3" {
			t.Fatalf("%s received %+v, want the one notice from 1", name, msgs)
		}
	}
	if ia.batches() != 0 {
		t.Fatal("the origin must not hear its own broadcast")
	}
}

func TestTheQueueIsTheDurabilityAndTheAckIsThePost(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b := room.Add(1), room.Add(2)
	in := newInbox()
	b.Subscribe(ctx, in.take)
	for i := 0; i < 3; i++ {
		a.Publish(ctx, func(e error) {
			if e != nil {
				t.Fatalf("publish %d acked %v with no consumer running", i, e)
			}
		}, broadcast.NewMessage(1, true, evt.Notice{Source: "swarm", Text: "n"}))
	}
	if n := len(engine.Pending()); n != 3 {
		t.Fatalf("three events held for the consumer, got %d", n)
	}
	go engine.Start(ctx)
	defer engine.Stop()
	in.await(t, 3)
}

func TestOnePendingHeartbeatPerMember(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b := room.Add(1), room.Add(2)
	in := newInbox()
	b.Subscribe(ctx, in.take)
	for i := 0; i < 5; i++ {
		a.Publish(ctx, func(e error) {
			if e != nil {
				t.Fatal(e)
			}
		}, broadcast.Heartbeat(1, true))
	}
	if n := len(engine.Pending()); n != 1 {
		t.Fatalf("five heartbeats before the consumer runs are one event, got %d", n)
	}
	a.Publish(ctx, func(error) {}, broadcast.NewMessage(1, true, evt.Notice{Source: "swarm", Text: "real"}))
	if n := len(engine.Pending()); n != 2 {
		t.Fatalf("a real message is never coalesced, got %d", n)
	}
	go engine.Start(ctx)
	defer engine.Stop()
	in.await(t, 2)
	a.Publish(ctx, func(error) {}, broadcast.Heartbeat(1, true))
	in.await(t, 3)
	if in.got[0][0].Signal() != nil || in.got[2][0].Signal() != nil {
		t.Fatal("after the first heartbeat ran, the next one posts again")
	}
}

func TestALeftMemberIsNamedInTheMiss(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b, c := room.Add(1), room.Add(2), room.Add(3)
	ib := newInbox()
	b.Subscribe(ctx, ib.take)
	closedC := room.Add(3)
	if closedC != c {
		t.Fatal("add of a present id is the same member")
	}
	go engine.Start(ctx)
	defer engine.Stop()
	c.Leave()
	if ids := room.Members(); len(ids) != 2 {
		t.Fatalf("after the leave members = %v", ids)
	}
	var ack error
	a.Publish(ctx, func(e error) { ack = e }, broadcast.NewMessage(1, true, evt.Notice{Source: "x", Text: "y"}))
	if ack != nil {
		t.Fatalf("a member that left is not a target, got %v", ack)
	}
	ib.await(t, 1)
}

func TestAClosedTransportRefusesAndIsNamed(t *testing.T) {
	engine := evt.NewEngine()
	tr := broadcast.NewLoopTransport(9, engine, prioRoom)
	tr.Close()
	var ack error
	tr.Send(context.Background(), func(e error) { ack = e }, broadcast.Heartbeat(9, true))
	if ack != context.Canceled {
		t.Fatalf("a send on a closed transport acks the close, got %v", ack)
	}
	var got error
	tr.Recv(context.Background(), func(e error, _ ...broadcast.Message) { got = e })
	if got != context.Canceled {
		t.Fatalf("a receive on a closed transport reports the close, got %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var canceled error
	broadcast.NewLoopTransport(8, engine, prioRoom).Send(ctx, func(e error) { canceled = e }, broadcast.Heartbeat(8, true))
	if canceled != context.Canceled {
		t.Fatalf("a canceled context never posts, got %v", canceled)
	}
}

func TestASubscriberCancelHearsItAndStopsReceiving(t *testing.T) {
	room, engine := loopRoom(t)
	a, b := room.Add(1), room.Add(2)
	in := newInbox()
	subCtx, cancel := context.WithCancel(context.Background())
	b.Subscribe(subCtx, in.take)
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		in.mu.Lock()
		n := len(in.errs)
		in.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-in.seen:
		case <-deadline:
			t.Fatal("the subscriber must hear its own cancel once")
		}
	}
	go engine.Start(context.Background())
	defer engine.Stop()
	a.Publish(context.Background(), func(error) {}, broadcast.NewMessage(1, true, evt.Notice{Source: "swarm", Text: "late"}))
	time.Sleep(50 * time.Millisecond)
	if in.batches() != 0 {
		t.Fatal("a canceled subscriber receives nothing more")
	}
}

func TestAHeartbeatAndASnapshotFromOneSenderWaitApart(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b := room.Add(1), room.Add(2)
	in := newInbox()
	b.Subscribe(ctx, in.take)
	for i := 0; i < 3; i++ {
		a.Publish(ctx, func(error) {}, broadcast.Heartbeat(1, true))
	}
	for i := 1; i <= 3; i++ {
		a.Publish(ctx, func(error) {}, broadcast.NewMessage(1, true, workers{Pending: i}))
	}
	if n := len(engine.Pending()); n != 2 {
		t.Fatalf("a heartbeat and a snapshot replace their own waiting value only: got %d", n)
	}
	go engine.Start(ctx)
	defer engine.Stop()
	in.await(t, 2)
	if in.got[0][0].Signal() != nil {
		t.Fatal("the heartbeat lands first and empty")
	}
	if st := in.got[1][0].Signal().(workers); st.Pending != 3 {
		t.Fatalf("the snapshot lands with the latest value, got %d", st.Pending)
	}
}

func TestMembershipIsSortedAndRemoveIsHonest(t *testing.T) {
	room, _ := loopRoom(t)
	room.Add(9)
	room.Add(2)
	room.Add(5)
	if ids := room.Members(); len(ids) != 3 || ids[0] != 2 || ids[1] != 5 || ids[2] != 9 {
		t.Fatalf("members = %v, want sorted ids", ids)
	}
	if !room.Remove(2) || room.Remove(2) {
		t.Fatal("the first remove is true, the second false")
	}
}

func TestASnapshotKeepsOnePendingPerSenderWithTheLatestValue(t *testing.T) {
	room, engine := loopRoom(t)
	ctx := context.Background()
	a, b := room.Add(1), room.Add(2)
	in := newInbox()
	b.Subscribe(ctx, in.take)
	for i := 1; i <= 4; i++ {
		a.Publish(ctx, func(error) {}, broadcast.NewMessage(1, true, workers{Pending: i}))
	}
	a.Publish(ctx, func(error) {}, broadcast.NewMessage(1, true, evt.Notice{Source: "swarm", Text: "a story, not a state"}))
	room.Add(3).Publish(ctx, func(error) {}, broadcast.NewMessage(3, true, workers{Pending: 9}))
	if n := len(engine.Pending()); n != 4 {
		t.Fatalf("four snapshots from one sender are one event to b, the notice is its own, the third member's snapshot is one each to a and b: got %d", n)
	}
	go engine.Start(ctx)
	defer engine.Stop()
	in.await(t, 3)
	if st := in.got[0][0].Signal().(workers); st.Pending != 4 {
		t.Fatalf("the frame that lands is the latest, got %d", st.Pending)
	}
}

func TestPipeTransportCarriesFramesAcrossAnOSPipeAndReportsTheEndOnce(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	enc := testEncoder{}
	child := broadcast.NewPipeTransport(4, w, enc)
	parent := broadcast.NewPipeTransport(4, r, enc)
	var mu sync.Mutex
	var got []broadcast.Message
	ends := 0
	parent.Recv(context.Background(), func(err error, messages ...broadcast.Message) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			ends++
			return
		}
		got = append(got, messages...)
	})
	acks := 0
	child.Send(context.Background(), func(ack error) {
		if ack != nil {
			t.Errorf("ack = %v", ack)
		}
		acks++
	}, broadcast.Heartbeat(4, true), broadcast.NewMessage(4, true, evt.Notice{Source: "w4", Text: "hi"}))
	if acks != 1 {
		t.Fatalf("one send is one ack, got %d", acks)
	}
	child.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n, e := len(got), ends
		mu.Unlock()
		if e == 1 {
			if n != 2 || got[0].Signal() != nil || got[1].Signal().(evt.Notice).Text != "hi" {
				t.Fatalf("frames = %d %+v, want the heartbeat then the notice", n, got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reader never reported the end: frames %d ends %d", n, e)
		}
		time.Sleep(time.Millisecond)
	}
	child.Send(context.Background(), func(ack error) {
		if ack == nil {
			t.Fatal("a closed pipe refuses the send")
		}
	}, broadcast.Heartbeat(4, true))
}

func TestPipeTransportClosesOnAFrameItCannotDecode(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	parent := broadcast.NewPipeTransport(4, r, testEncoder{})
	end := make(chan error, 1)
	parent.Recv(context.Background(), func(err error, messages ...broadcast.Message) {
		if err != nil {
			end <- err
		}
	})
	io.WriteString(w, "not a frame\n")
	select {
	case err := <-end:
		if !strings.Contains(err.Error(), "test:") {
			t.Fatalf("end = %v, want the decode named", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a bad frame must end the receive")
	}
	if _, err := w.Write([]byte("x\n")); err == nil {
		t.Fatal("the read end is closed after a bad frame, so the write fails")
	}
}

func TestMintIssuesIdsBelowEveryMemberAndNeverReissues(t *testing.T) {
	engine := evt.NewEngine()
	room := broadcast.NewRoom("fleet", func(id int64) broadcast.Transport {
		return broadcast.NewLoopTransport(id, engine, 0)
	})
	room.Add(-1)
	room.Add(-4)
	room.Add(3)
	first := room.Mint()
	if first.Id() != -5 {
		t.Fatalf("the first minted id is below the lowest named one, got %d", first.Id())
	}
	first.Leave()
	second := room.Mint()
	if second.Id() != -6 {
		t.Fatalf("a left member's id is never reissued, got %d", second.Id())
	}
	room.Add(-9)
	if third := room.Mint(); third.Id() != -10 {
		t.Fatalf("a named member below the floor moves it, got %d", third.Id())
	}
}
