package broadcast_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

type delta struct {
	Text string

	evt.Mark
}

type ghost struct {
	Why string

	evt.Mark
}

func wireRegistry(kinds ...string) broadcast.Registry {
	reg := broadcast.NewRegistry()
	var err error
	for _, kind := range kinds {
		switch kind {
		case "notice":
			reg, err = broadcast.Declare(reg, "notice",
				func(n evt.Notice) ([]byte, error) { return json.Marshal(n) },
				func(b []byte) (evt.Notice, error) {
					var n evt.Notice
					return n, json.Unmarshal(b, &n)
				})
		case "status":
			reg, err = broadcast.Declare(reg, "status",
				func(w workers) ([]byte, error) { return json.Marshal(w) },
				func(b []byte) (workers, error) {
					var w workers
					return w, json.Unmarshal(b, &w)
				})
		case "delta":
			reg, err = broadcast.Declare(reg, "delta",
				func(d delta) ([]byte, error) { return json.Marshal(d) },
				func(b []byte) (delta, error) {
					var d delta
					return d, json.Unmarshal(b, &d)
				})
		}
		if err != nil {
			panic(err)
		}
	}
	return reg
}

func TestTheEncoderRoundTripsTheRegisteredKinds(t *testing.T) {
	enc := broadcast.NewJSONEncoder(wireRegistry("notice", "status", "delta"))
	for _, m := range []broadcast.Message{
		broadcast.NewMessage(4, true, evt.Notice{Source: "decision", Text: "review: settled 10"}),
		broadcast.NewMessage(4, true, workers{Pending: 7, Names: []string{"t3"}}),
		broadcast.NewMessage(4, true, delta{Text: "weighing"}),
		broadcast.Heartbeat(4, false),
	} {
		raw, err := enc.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		back, err := enc.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if back.Origin() != 4 || back.Ok() != m.Ok() {
			t.Fatalf("round trip lost the envelope: %+v", back)
		}
		switch want := m.Signal().(type) {
		case nil:
			if back.Signal() != nil {
				t.Fatal("a heartbeat decodes to no signal")
			}
		case workers:
			got := back.Signal().(workers)
			if got.Pending != want.Pending || len(got.Names) != len(want.Names) || got.Names[0] != want.Names[0] {
				t.Fatalf("status round trip = %+v", got)
			}
		default:
			if back.Signal() != want {
				t.Fatalf("round trip = %+v, want %+v", back.Signal(), want)
			}
		}
	}
}

func TestTheFrameIsTheShapeTheWireSpeaks(t *testing.T) {
	enc := broadcast.NewJSONEncoder(wireRegistry("notice"))
	raw, err := enc.Encode(broadcast.NewMessage(4, true, evt.Notice{Source: "w4", Text: "hi"}))
	if err != nil {
		t.Fatal(err)
	}
	frame := `{"origin":4,"ok":true,"kind":"notice","payload":{"Source":"w4","Text":"hi","Level":0}}`
	if string(raw) != frame {
		t.Fatalf("frame = %s, want %s", raw, frame)
	}
	beat, err := enc.Encode(broadcast.Heartbeat(4, true))
	if err != nil {
		t.Fatal(err)
	}
	if string(beat) != `{"origin":4,"ok":true}` {
		t.Fatalf("heartbeat = %s, want the frame with no kind", beat)
	}
}

func TestAnUnregisteredTypeErrorsAtTheSender(t *testing.T) {
	enc := broadcast.NewJSONEncoder(wireRegistry("notice"))
	_, err := enc.Encode(broadcast.NewMessage(1, true, delta{Text: "x"}))
	if err == nil || !strings.Contains(err.Error(), "does not cross a transport") {
		t.Fatalf("an unregistered type refuses by name, got %v", err)
	}
	_, err = enc.Encode(broadcast.NewMessage(1, true, ghost{Why: "x"}))
	if err == nil || !strings.Contains(err.Error(), "ghost does not cross a transport") {
		t.Fatalf("the error names the type, got %v", err)
	}
}

func TestAnUnknownKindErrorsAtTheReceiver(t *testing.T) {
	enc := broadcast.NewJSONEncoder(wireRegistry())
	if _, err := enc.Decode([]byte(`{"origin":1,"ok":true,"kind":"nope","payload":{}}`)); err == nil || !strings.Contains(err.Error(), `unknown kind "nope"`) {
		t.Fatalf("an unknown kind refuses by name, got %v", err)
	}
}

func TestDeclareRefusesADuplicateKindOrType(t *testing.T) {
	reg := wireRegistry("notice")
	if _, err := broadcast.Declare(reg, "notice",
		func(n evt.Notice) ([]byte, error) { return json.Marshal(n) },
		func(b []byte) (evt.Notice, error) { var n evt.Notice; return n, json.Unmarshal(b, &n) }); err == nil || !strings.Contains(err.Error(), `registered twice`) {
		t.Fatalf("a duplicate kind refuses, got %v", err)
	}
	if _, err := broadcast.Declare(reg, "notice2",
		func(n evt.Notice) ([]byte, error) { return json.Marshal(n) },
		func(b []byte) (evt.Notice, error) { var n evt.Notice; return n, json.Unmarshal(b, &n) }); err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("a duplicate type under a new kind refuses, got %v", err)
	}
}

func TestARegistryIsNotMutatedByDeclare(t *testing.T) {
	reg := wireRegistry("notice")
	if _, err := broadcast.Declare(reg, "delta",
		func(d delta) ([]byte, error) { return json.Marshal(d) },
		func(b []byte) (delta, error) { var d delta; return d, json.Unmarshal(b, &d) }); err != nil {
		t.Fatal(err)
	}
	enc := broadcast.NewJSONEncoder(reg)
	if _, err := enc.Encode(broadcast.NewMessage(1, true, delta{Text: "x"})); err == nil {
		t.Fatal("the handed-over registry must not grow after the declare")
	}
}

func TestARoomOverAPipeCarriesSignalsTheFarEndNeverHeardOf(t *testing.T) {
	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, w1, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	regB := wireRegistry("notice", "status")
	regEar := wireRegistry("status")
	transportFor := map[int64]broadcast.Transport{
		1: broadcast.NewPipeTransport(1, w1, broadcast.NewJSONEncoder(regEar)),
		2: broadcast.NewPipeTransport(2, w2, broadcast.NewJSONEncoder(regB)),
	}
	room := broadcast.NewRoom("fleet", func(origin int64) broadcast.Transport { return transportFor[origin] })
	a := room.Add(1)
	room.Add(2)
	ctx := context.Background()
	ear := &inbox{seen: make(chan struct{}, 64)}
	earTransport := broadcast.NewPipeTransport(9, r2, broadcast.NewJSONEncoder(regEar))
	earTransport.Recv(ctx, ear.take)

	var ack error
	a.Publish(ctx, func(e error) { ack = e }, broadcast.NewMessage(1, true, workers{Pending: 3}))
	if ack != nil {
		t.Fatalf("a registered kind crosses the pipe: %v", ack)
	}
	ear.await(t, 1)
	if st := ear.got[0][0].Signal().(workers); st.Pending != 3 || ear.got[0][0].Origin() != 1 {
		t.Fatalf("the snapshot does not survive the pipe: %+v", ear.got[0][0])
	}

	a.Publish(ctx, func(e error) { ack = e }, broadcast.NewMessage(1, true, evt.Notice{Source: "w1", Text: "hi"}))
	if ack != nil {
		t.Fatalf("the member's own kind still crosses: %v", ack)
	}
	deadline := time.After(2 * time.Second)
	for {
		ear.mu.Lock()
		errs := len(ear.errs)
		ear.mu.Unlock()
		if errs == 1 {
			if !strings.Contains(ear.errs[0].Error(), `unknown kind "notice"`) {
				t.Fatalf("the ear reports the kind by name, got %v", ear.errs[0])
			}
			break
		}
		select {
		case <-ear.seen:
		case <-deadline:
			t.Fatal("the ear must report the unknown kind once")
		}
	}

	a.Publish(ctx, func(e error) { ack = e }, broadcast.NewMessage(1, true, ghost{Why: "because"}))
	if ack == nil || !strings.Contains(ack.Error(), "broadcast missed 2") || !strings.Contains(ack.Error(), "ghost does not cross a transport") {
		t.Fatalf("an unregistered type is the sender's miss, named: %v", ack)
	}
}
