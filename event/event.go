package event

type Event interface{ event() }

type Marker struct{}

func (Marker) event() {}

type Snapshot interface {
	Event
	Snapshot()
}
