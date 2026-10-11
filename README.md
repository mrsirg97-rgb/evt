# evt

0.1.0: the base package for building event-based systems in Go. Three
things and nothing about agents: an engine that runs closures on one
consumer in priority order, an event, which is an interface, and a room
whose members exchange events over a transport. Anything that names a
model, a tool, a turn or a transcript is an application of the module,
not part of it.

    go get github.com/mrsirg97-rgb/evt

The module imports nothing outside the standard library.

## the engine

```go
engine := evt.NewEngine()
scheduler := evt.NewScheduler(engine)
scheduler.Start()
scheduler.Schedule(evt.Func(func(ctx context.Context) {
    // the consumer is the only thing that touches the session
}), 30)
```

One consumer pops events highest priority first, arrival order within a
priority, and executes outside the queue's lock. `Add` is safe from any
goroutine; `Update(id, priority)` reprioritizes a pending event in place;
there are no locks beyond the queue's own, no timeouts, no polls. An
event may `Add`, may block, may run long; a panic in an event is the
closure's, not recovered.

## one event

An event is an interface whose method is unexported, so a type declares
itself an event by carrying the module's marker:

```go
type delta struct {
	Text string

	event.Marker
}

type workers struct {
	Pending int

	event.Marker
}

func (workers) Snapshot() {}
```

`delta` and `workers` are events now: `event.Event` accepts them and
`event.Snapshot` accepts `workers` — a state, not a story. The marker
carries no bytes: the JSON of a carrying type is the JSON of the plain
one, pinned by a test.

## one room

A room's members exchange messages that carry an origin, the member's
health and an event; a message with no event is a heartbeat. The loop
transport posts to an engine; the pipe transport crosses a process.

```go
engine := evt.NewEngine()
room := broadcast.NewRoom("fleet", func(origin int64) broadcast.Transport {
	return broadcast.NewLoopTransport(origin, engine, 30)
})
worker := room.Add(1)
broadcast.Say(worker, "swarm", "claimed t3", event.LevelInfo)
```

Across a process the wire is one JSON frame per line — origin, ok, kind,
payload — and the application registers each event type it sends, once,
by name:

```go
reg, err := broadcast.Declare(broadcast.NewRegistry(), "notice",
	func(n event.Notice) ([]byte, error) { return json.Marshal(n) },
	func(b []byte) (event.Notice, error) {
		var n event.Notice
		return n, json.Unmarshal(b, &n)
	})
tr := broadcast.NewPipeTransport(4, w, broadcast.NewJSONEncoder(reg))
```

An unregistered type on `Send` is an error at the sender; an unknown kind
on `Recv` is an error the receiver reports once. The registry is built
whole and handed to the codec when it is constructed — nothing global,
nothing mutated after start.

## what it is not

No model, no tool, no turn, no session, no transcript: those are the
application's. rig (github.com/mrsirg97-rgb/rig) is the first
application: its turn loop is this engine's consumer, its fleet is this
room, and its wire is this frame. evtjs hosts JavaScript on the same
engine without the engine changing.

Packages: `evt` the engine, `event` the types, `broadcast` the room —
each carries its PACKAGE.md; `specs/SPEC_EVT.md` is the engine's spec
and `specs/DESIGN_EVT.md` this module's design note.
