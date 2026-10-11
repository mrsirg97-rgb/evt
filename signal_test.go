package evt_test

import (
	"encoding/json"
	"testing"

	"github.com/mrsirg97-rgb/evt"
)

type delta struct {
	Text string

	evt.Mark
}

type workers struct {
	Count int

	evt.Mark
}

func (workers) Snapshot() {}

func TestAnApplicationDeclaresItsOwnSignal(t *testing.T) {
	var s evt.Signal = delta{Text: "t"}
	if _, ok := s.(delta); !ok {
		t.Fatalf("the application type does not satisfy the signal interface")
	}
}

func TestAnApplicationDeclaresItsOwnSnapshot(t *testing.T) {
	var s evt.Snapshot = workers{Count: 3}
	if _, ok := s.(workers); !ok {
		t.Fatalf("the application type does not satisfy the snapshot interface")
	}
}

func TestTheMarkMovesNoBytes(t *testing.T) {
	plain, _ := json.Marshal(struct{ Text string }{Text: "t"})
	carrying, _ := json.Marshal(delta{Text: "t"})
	if string(plain) != string(carrying) {
		t.Fatalf("the mark changed the payload: %s vs %s", plain, carrying)
	}
}

func TestTheLevelNamesTheVoice(t *testing.T) {
	cases := map[evt.Level]string{
		evt.LevelInfo:    "info",
		evt.LevelSuccess: "success",
		evt.LevelError:   "error",
		evt.Level(9):     "info",
	}
	for level, want := range cases {
		if got := level.String(); got != want {
			t.Fatalf("Level(%d).String() = %q, want %q", level, got, want)
		}
	}
}

func TestTheNoticeIsASignal(t *testing.T) {
	var s evt.Signal = evt.Notice{Source: "graph", Text: "indexed", Level: evt.LevelError}
	n, ok := s.(evt.Notice)
	if !ok || n.Source != "graph" || n.Level != evt.LevelError {
		t.Fatalf("the notice does not carry its voice")
	}
}
