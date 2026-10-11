# evt

The base package for building event-based systems in Go. Four things and
nothing about agents:

- **an engine**: closures run on one consumer, highest priority first,
  arrival order within a priority;
- **a signal**: an interface for what happened, told to whoever listens;
- **a room** (`broadcast`): members exchanging signals over a transport,
  in one process or across a pipe;
- **a hand-off**: a cap on how much work starts at once.

    go get github.com/mrsirg97-rgb/evt

```go
engine := evt.NewEngine()
scheduler := evt.NewScheduler(engine)
scheduler.Start()
scheduler.Schedule(evt.Func(func(ctx context.Context) {
	fmt.Println("runs on the one consumer")
}), 30)
```

```go
type delta struct {
	Text string

	evt.Mark
}
```

`delta` is a signal now. `evt.Event` is work the engine runs;
`evt.Signal` is what an application tells.

Standard library only. Each package carries a `PACKAGE.md`;
`specs/SPEC_EVT.md` is the engine's spec and `specs/DESIGN_EVT.md` the
module's design note. [rig](https://github.com/mrsirg97-rgb/rig) is the
first application.
