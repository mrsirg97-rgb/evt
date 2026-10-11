package broadcast

import (
	"github.com/mrsirg97-rgb/evt"
)

type Message interface {
	Origin() int64
	Ok() bool
	Signal() evt.Signal
}

type message struct {
	origin int64
	ok     bool
	signal evt.Signal
}

func NewMessage(origin int64, ok bool, signal ...evt.Signal) Message {
	message := &message{origin: origin, ok: ok}
	if len(signal) > 0 {
		message.signal = signal[0]
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

func (m *message) Signal() evt.Signal {
	return m.signal
}
