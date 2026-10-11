package broadcast

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mrsirg97-rgb/evt"
)

type codec struct {
	kind   string
	encode func(evt.Signal) ([]byte, error)
	decode func([]byte) (evt.Signal, error)
}

type Registry interface {
	byType(typ reflect.Type) (codec, bool)
	byKind(kind string) (codec, bool)
	with(typ reflect.Type, c codec) Registry
}

type registry struct {
	types map[reflect.Type]codec
	kinds map[string]codec
}

func NewRegistry() Registry {
	return &registry{types: map[reflect.Type]codec{}, kinds: map[string]codec{}}
}

func (r *registry) byType(typ reflect.Type) (codec, bool) {
	c, ok := r.types[typ]
	return c, ok
}

func (r *registry) byKind(kind string) (codec, bool) {
	c, ok := r.kinds[kind]
	return c, ok
}

func (r *registry) with(typ reflect.Type, c codec) Registry {
	next := &registry{
		types: map[reflect.Type]codec{typ: c},
		kinds: map[string]codec{c.kind: c},
	}
	for t, k := range r.types {
		next.types[t] = k
	}
	for k, v := range r.kinds {
		next.kinds[k] = v
	}
	return next
}

func Declare[T evt.Signal](r Registry, kind string, encode func(T) ([]byte, error), decode func([]byte) (T, error)) (Registry, error) {
	if r == nil {
		r = NewRegistry()
	}
	var sample T
	typ := reflect.TypeOf(sample)
	if typ == nil {
		return nil, fmt.Errorf("broadcast: %q declares an interface, not a signal type", kind)
	}
	if _, dup := r.byType(typ); dup {
		return nil, fmt.Errorf("broadcast: %T is registered twice", sample)
	}
	if _, dup := r.byKind(kind); dup {
		return nil, fmt.Errorf("broadcast: %q is registered twice", kind)
	}
	return r.with(typ, codec{
		kind: kind,
		encode: func(ev evt.Signal) ([]byte, error) {
			return encode(ev.(T))
		},
		decode: func(raw []byte) (evt.Signal, error) {
			return decode(raw)
		},
	}), nil
}

type jsonMessage struct {
	Origin  int64           `json:"origin"`
	Ok      bool            `json:"ok"`
	Kind    string          `json:"kind,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewJSONEncoder(reg Registry) Encoder {
	if reg == nil {
		reg = NewRegistry()
	}
	return &jsonEncoder{reg: reg}
}

type jsonEncoder struct {
	reg Registry
}

func (e *jsonEncoder) Encode(message Message) ([]byte, error) {
	frame := jsonMessage{Origin: message.Origin(), Ok: message.Ok()}
	if ev := message.Signal(); ev != nil {
		c, ok := e.reg.byType(reflect.TypeOf(ev))
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
	var frame jsonMessage
	if err := json.Unmarshal(encoded, &frame); err != nil {
		return nil, err
	}
	if frame.Kind == "" {
		return NewMessage(frame.Origin, frame.Ok), nil
	}
	c, ok := e.reg.byKind(frame.Kind)
	if !ok {
		return nil, fmt.Errorf("broadcast: unknown kind %q", frame.Kind)
	}
	ev, err := c.decode(frame.Payload)
	if err != nil {
		return nil, err
	}
	return NewMessage(frame.Origin, frame.Ok, ev), nil
}
