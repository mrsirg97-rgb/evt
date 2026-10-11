package event_test

import (
	"testing"

	"github.com/mrsirg97-rgb/evt/event"
)

func TestTheLevelNamesTheVoice(t *testing.T) {
	cases := map[event.Level]string{
		event.LevelInfo:    "info",
		event.LevelSuccess: "success",
		event.LevelError:   "error",
		event.Level(9):     "info",
	}
	for level, want := range cases {
		if got := level.String(); got != want {
			t.Fatalf("Level(%d).String() = %q, want %q", level, got, want)
		}
	}
}

func TestTheNoticeIsAnEvent(t *testing.T) {
	var e event.Event = event.Notice{Source: "graph", Text: "indexed", Level: event.LevelError}
	n, ok := e.(event.Notice)
	if !ok || n.Source != "graph" || n.Level != event.LevelError {
		t.Fatalf("the notice does not carry its voice")
	}
}
