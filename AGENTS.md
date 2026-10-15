# evt

## What it is

The event loop (SPEC_EVT): libevt's shape made Go-centric. One
consumer, many producers; work arrives as closures ordered by priority
then arrival; the consumer executes one at a time outside the queue's
lock. A leaf, stdlib only; the module's root package, so
`import ".../evt"` is the engine. Beside the engine sit the two things
built on it that every application needs: the signal (what happened,
told to whoever listens) and the hand-off (a cap on how much work
starts at once).

## What it includes

- `Closure`: the unit of work: `Resolve(ctx)`; `Func` adapts a
  `func(context.Context)`. (libevt's `Context`; renamed, since in Go
  that word is `context.Context`.)
- `Event`: `ID`, `Priority`, `Closure`; `NewEvent`;
  `Execute(ctx, e)` resolves its closure.
- `Clock`: `Step() uint64` mints ids: `Counter()` (the default, a
  strictly increasing atomic) and `Monotonic()` (wall nanoseconds, as
  libevt's `clock_step`, but never repeating: a same-nanosecond step
  takes last+1, so a push is never dropped as a duplicate id).
- `Queue`: the 4-ary max-heap ordered `(priority desc, id asc)` with a
  position map: `Push`, `Pop`, `Peek`, `Update` (the one addition:
  reprioritize in place), `View` (a sorted snapshot drained from a
  clone), `Len`. Not goroutine-safe by itself, as in C; the engine is
  the lock.
- `Engine`: `Start(ctx)` (blocks; the consumer), `Add` (any goroutine),
  `Update`, `Stop`, `Pending`; options `WithClock`, `WithTick` (the idle
  hook, libevt's `tick_fn`; absent, the consumer waits on a cond),
  `WithCapacity`.
- `Scheduler`: the harness: `Start() error` (`ErrNoEngine`,
  `ErrStarted`), `Schedule`, `Stop` (cancel, join: the ctx does the
  stop), `Done`.
- `signal.go`: `Signal`, an interface whose method is unexported, so a
  type is a signal only by carrying `Mark` (embed it; it adds no bytes
  to the type's JSON). `Snapshot` is a signal that is a state and not a
  story. `Notice` (`Source`, `Text`, `Level`) is the one voice a
  background subsystem speaks with; `Level` is info, success or error.
  `Event` is work the engine runs; `Signal` is what an application
  tells: the two words never mean the same thing.
- `handoff.go`: `Cap` and `Submission`, both interfaces.
  `NewCap(engine, workers)` (zero is no cap) and
  `Submit(ctx, work, priority)`. A submission that `Started`
  holds a place and the submitter runs the work itself; a queued one is
  resolved by the cap on its own goroutine when a place frees, with the
  submitter's context. `Release` frees the place and starts the next
  queued work, in submit order; `Stop` settles a queued work with a
  stopped context and never starts it; `Settled` closes either way.
  `Size`, `InFlight`, `Queued` report what the cap holds. The queue is
  touched only by the consumer.

## How it is consumed

- `e := evt.NewEngine(): s := evt.NewScheduler(e); s.Start();
  s.Schedule(evt.Func(func(ctx) { … }), priority)`; `s.Stop()` joins.
- Or drive the engine on a goroutine of your own: `go e.Start(ctx)`;
  cancel the ctx or call `e.Stop()`.
- A higher priority runs first: equal priorities run in arrival order;
  a negative priority clamps to 0.

## Gotchas

- The queue alone is not safe for concurrent use (libevt's contract);
  go through the engine, which holds the mutex around every queue op.
- Execution is outside the lock: an event may `Add`, may block, may run
  long; a panic in an event is the closure's, not recovered.
- `Stop` runs nothing further: the event in flight finishes; `Pending`
  shows the rest, sorted; an `Add` after `Stop` refuses (no id, nothing
  queued), so a producer of a dead engine cannot grow it. Cancelling
  `Start`'s ctx is a `Stop`.
- An engine runs once: a second `Start` on a running or stopped engine
  panics, naming the rule. The scheduler refuses earlier with
  `ErrStarted`, and its `Stop` is a cancel and a join (the ctx does the
  stop, so it never halts an engine that has not begun).
- `Update` on an id that already popped is `false`, never an error.
- `Push` of a duplicate id is ignored (the position map is the identity).
- Priority starves: a stream of high events never yields to low ones.
  The consumer that assigns priorities owns that (SPEC_EVT phase 2).
