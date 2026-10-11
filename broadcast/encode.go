package broadcast

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mrsirg97-rgb/evt/event"
)

type codec struct {
	kind   string
	encode func(event.Event) ([]byte, error)
	decode func([]byte) (event.Event, error)
}

type Registry struct {
	byType map[reflect.Type]codec
	byKind map[string]codec
}

func NewRegistry() Registry {
	return Registry{byType: map[reflect.Type]codec{}, byKind: map[string]codec{}}
}

func Declare[T event.Event](r Registry, kind string, encode func(T) ([]byte, error), decode func([]byte) (T, error)) (Registry, error) {
	var sample T
	typ := reflect.TypeOf(sample)
	if typ == nil {
		return Registry{}, fmt.Errorf("broadcast: %q declares an interface, not an event type", kind)
	}
	if _, dup := r.byType[typ]; dup {
		return Registry{}, fmt.Errorf("broadcast: %T is registered twice", sample)
	}
	if _, dup := r.byKind[kind]; dup {
		return Registry{}, fmt.Errorf("broadcast: %q is registered twice", kind)
	}
	c := codec{
		kind: kind,
		encode: func(ev event.Event) ([]byte, error) {
			return encode(ev.(T))
		},
		decode: func(raw []byte) (event.Event, error) {
			return decode(raw)
		},
	}
	next := Registry{
		byType: map[reflect.Type]codec{typ: c},
		byKind: map[string]codec{kind: c},
	}
	for t, k := range r.byType {
		next.byType[t] = k
	}
	for k, v := range r.byKind {
		next.byKind[k] = v
	}
	return next, nil
}

type JSONMessage struct {
	Origin  int64           `json:"origin"`
	Ok      bool            `json:"ok"`
	Kind    string          `json:"kind,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewJSONEncoder(reg Registry) Encoder {
	return &jsonEncoder{reg: reg}
}

type jsonEncoder struct {
	reg Registry
}

func (e *jsonEncoder) Encode(message Message) ([]byte, error) {
	frame := JSONMessage{Origin: message.Origin(), Ok: message.Ok()}
	if ev := message.Event(); ev != nil {
		c, ok := e.reg.byType[reflect.TypeOf(ev)]
		if !ok {
			return nil, fmt.Errorf("broadcast: %T does not cross a transport", ev)
		}
		payload, err := c.encode(ev)
		if err != nil {
			return nil, err
		}
		frame.Kind, frame.Payload = c.kind, payload
	}
	return json.Marshal(frame)
}

func (e *jsonEncoder) Decode(encoded []byte) (Message, error) {
	var frame JSONMessage
	if err := json.Unmarshal(encoded, &frame); err != nil {
		return nil, err
	}
	if frame.Kind == "" {
		return NewMessage(frame.Origin, frame.Ok), nil
	}
	c, ok := e.reg.byKind[frame.Kind]
	if !ok {
		return nil, fmt.Errorf("broadcast: unknown kind %q", frame.Kind)
	}
	ev, err := c.decode(frame.Payload)
	if err != nil {
		return nil, err
	}
	return NewMessage(frame.Origin, frame.Ok, ev), nil
}
