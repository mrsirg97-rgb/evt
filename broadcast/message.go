package broadcast

import (
	"github.com/mrsirg97-rgb/evt/event"
)

type Message interface {
	Origin() int64
	Ok() bool
	Event() event.Event
}

type message struct {
	origin int64
	ok     bool
	event  event.Event
}

func NewMessage(origin int64, ok bool, event ...event.Event) Message {
	message := &message{origin: origin, ok: ok}
	if len(event) > 0 {
		message.event = event[0]
	}
	return message
}

func Heartbeat(origin int64, ok bool) Message {
	return NewMessage(origin, ok)
}

func (m *message) Origin() int64 {
	return m.origin
}

func (m *message) Ok() bool {
	return m.ok
}

func (m *message) Event() event.Event {
	return m.event
}
