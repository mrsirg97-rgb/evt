package event_test

import (
	"encoding/json"
	"testing"

	"github.com/mrsirg97-rgb/evt/event"
)

type delta struct {
	Text string

	event.Marker
}

type workers struct {
	Count int

	event.Marker
}

func (workers) Snapshot() {}

func TestAnApplicationDeclaresItsOwnEvent(t *testing.T) {
	var e event.Event = delta{Text: "t"}
	if _, ok := e.(delta); !ok {
		t.Fatalf("the application type does not satisfy the event interface")
	}
}

func TestAnApplicationDeclaresItsOwnSnapshot(t *testing.T) {
	var s event.Snapshot = workers{Count: 3}
	if _, ok := s.(workers); !ok {
		t.Fatalf("the application type does not satisfy the snapshot interface")
	}
}

func TestTheMarkerMovesNoBytes(t *testing.T) {
	plain, _ := json.Marshal(struct{ Text string }{Text: "t"})
	carrying, _ := json.Marshal(delta{Text: "t"})
	if string(plain) != string(carrying) {
		t.Fatalf("the marker changed the payload: %s vs %s", plain, carrying)
	}
}
