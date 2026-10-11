# evt 0.1.0: the engine, the signal, the room and the hand-off leave rig as one module

Design note for the module `github.com/mrsirg97-rgb/evt`, written against
rig main after 2.17.1 (74ab79f). The counts below were re-verified at that
tree before this note was written.

## the rule

The base package for building event-based systems in Go is four things
and nothing about agents:

1. **an engine** that runs closures on one consumer, highest priority
   first, arrival order within a priority, no locks, timeouts or polls
   anywhere;
2. **a signal**, which is an interface: what happened, told to whoever
   listens (the engine's `Event` is the other word: work it runs);
3. **a room** whose members exchange signals over a transport;
4. **a hand-off**: a cap on how much work starts at once, its state
   touched only by the consumer (below).

Anything that names a model, a tool, a turn, a session or a transcript is
an application of the module and stays in the application. Rig keeps
`swarm` and `tool/delegate` (they are rig's fleet, written against rig's
stores); what is reusable in them is the pattern — a router handing work
to idle members of a room — and the README may say so in a paragraph.

## the packages

The root package is the engine, so `import ".../evt"` is the engine as it
is in rig today. The signal types live beside it in the root, in their
own file. The room is the subpackage `broadcast`.

- **`evt`** (root, `./`) — the engine, moved unchanged: `Closure`, `Func`,
  `Event` (`ID`, `Priority`, `Closure`), `NewEvent`, `Execute`, `Clock`,
  `Counter`, `Monotonic`, `Queue`, `NewQueue`, `Engine`, `NewEngine`,
  `WithClock`, `WithTick`, `WithCapacity`, `Scheduler`, `NewScheduler`,
  `ErrNoEngine`, `ErrStarted`. Same files (`clock.go`, `closure.go`,
  `engine.go`, `event.go`, `queue.go`, `scheduler.go`), same tests, same
  exported names. The only diff is the import path. Imports nothing.
  Beside the engine, `signal.go` holds the signal types: `Signal`,
  `Mark`, `Snapshot`, `Notice`, `Level` (`LevelInfo`, `LevelSuccess`,
  `LevelError`); and `handoff.go` the hand-off: `Cap`, `NewCap`,
  `Submission`.
- **`broadcast`** (`./broadcast`) — the room: `Message`, `NewMessage`,
  `Heartbeat`, `Transport`, `NewLoopTransport`, `NewPipeTransport`,
  `Member`, `NewMember`, `Room`, `NewRoom`, `Say`, the `Encoder`
  interface and its JSON codec with the wire `Registry`. Imports `evt`
  only.

No cycles: the root imports nothing, the room imports the root.

Why `Signal`, and why in the root: the engine already owns the word
`Event` — work it runs, with an id, a priority and a closure. What a
provider streams, a frontend is told and a room's members exchange is
the other thing: what happened, told to whoever listens. Two words for
two things, side by side in one package, so the difference is the first
thing a reader meets. The first cut put these types in a subpackage
`event` as `event.Event`; `evt.Event` beside `event.Event` named two
different things with one word, and the package went. A package named
`signal` was rejected: it would shadow `os/signal` in any file that
handles a SIGTERM. `core` was rejected: that word stays rig's.

## the moves, type by type

Every type in rig's `core`, one line each. The starting cut holds: the
event interface, the snapshot interface, the fleet's voice and its level
move; the agent vocabulary stays.

| type | verdict | why |
| --- | --- | --- |
| `Event` (`interface{ event() }`) | **moves**, as `evt.Signal` (`interface{ signal() }`) | the module's second thing: a signal is an interface, and the shape that lets applications declare one is decided below |
| `Snapshot` (`interface{ Event; Snapshot() }`) | **moves** | a room's state-vs-story distinction; not agent vocabulary |
| `Notice` | **moves** | the one voice every background subsystem speaks with — room vocabulary, not agent vocabulary |
| `Level` | **moves** | Notice's level; one concept, one home — moves with it |
| `Mark` (new) | **new, in the root** | the declaration shape (below) |
| `TextDelta`, `ReasoningDelta` | stays | names a model stream |
| `ToolCallEvent`, `ToolStart`, `ToolResult`, `ToolCall` | stays | names a tool |
| `Done`, `EmptyTurn`, `TurnReason`, `TurnEnd`, `Compacted`, `Compacting` | stays | names a turn |
| `Fault` | stays | a turn's fault; rides the stream |
| `Usage` | stays | names a model's budget |
| `TestEvent` | stays | the provider stream's test fixture; the loop's tests speak it |
| `SwarmWorker`, `SwarmStatus`, `WorkerDone` | stays | the fleet's agent vocabulary; `SwarmStatus` stays a `Snapshot` through the moved shape |
| `Verdict`, `Phase` | stays | agent decisions; they cross the wire as rig's registered types |
| `Provider`, `Request` | stays | names a model |
| `Role`, `Message` | stays | names the transcript |
| `Tool`, `ToolSpec` | stays | names a tool |
| `Frontend` | stays | the session's face; its `Notify` takes the moved `Event` through an import |
| `ContextPolicy`, `Session`, `FileState` | stays | names the session |
| `Command` | stays | names a command |
| `ToolExec`, `ToolMiddleware`, `ToolMiddlewareFunc`, `TurnObserver`, `GuidelineContributor` | stays | the loop's middleware; agent machinery |

No type in rig's `core` fits neither side; the note does not stop.

`broadcast`'s move is the same cut: `Message`, `Transport` (loop and
pipe), `Member`, `Room`, `Say` move with a generic payload — nothing in
them names an application's type. `Say` publishes `evt.Notice` and
keeps its variadic level, so a caller names the level where the text is
made, as rig does today.

## decision: how an application declares a signal

In rig's `core` the interface was `Event` with an unexported method, so
only `core` could declare one. One shape moves to the module; two
candidates:

1. an exported marker method — `type Signal interface{ Signal() }`,
   every application type carries `func (T) Signal() {}`;
2. **embedding** — `type Signal interface{ signal() }` stays closed, and
   the module exports the one type that opens it:
   `type Mark struct{}` with `func (Mark) signal() {}`. An
   application writes `evt.Mark` into its struct and the method is
   promoted from the module's own package.

The shape is **embedding**, for three reasons:

- **one declaration.** An exported marker restates the concept in every
  application type — seventeen empty methods in rig's `core` alone, one
  more per application type forever. Embedding states "is an event"
  once, in the module.
- **deny by default.** The unexported method means only types the
  module's marker vouches for satisfy the interface. An exported marker
  lets any struct with a stray `Event()` method — an accessor, say —
  become an event by accident; embedding is exact.
- **the registry handshake composes.** Registering a wire type asserts
  two opt-ins — it is a signal (the mark) and it crosses (the
  registry) — and the marker field is where the first one is written.

Verified before this note: a type in another package embedding an
exported empty struct from the marker's package satisfies the
unexported-method interface, and the composite snapshot
(`interface{ Event; Snapshot() }`) is satisfied the same way. Also
verified: `json.Marshal` of a struct carrying the empty marker produces
byte-identical output, so the fleet pipe's payloads do not move. Inside
the root package itself, `Notice` declares the method directly (it is
in the package); the marker exists for out-of-package declarers. One
method, one concept, two spellings, one package boundary.

## decision: the wire

The frame stays what it is: origin, ok, kind, payload, JSON, one frame
per line. The pipe encoder no longer lists types. Instead:

- **`broadcast.Registry`** maps a kind to how its event encodes and
   decodes. It is built whole — every `Declare` returns a fresh
   registry, nothing mutates in place — and is handed to the JSON codec
   when the codec is constructed: `NewJSONEncoder(reg)`. The loop
   transport takes no registry: it never crosses a process.
- **registration is once, by name**: an application registers each
   event type it sends with the kind string that names it on the wire
   and the two directions. Duplicate kinds and duplicate types refuse at
   build; the registry is closed after that.
- **an unregistered type on `Send` is an error at the sender**, naming
   the type; **an unknown kind on `Recv` is an error the receiver
   reports once** — the pipe closes and reports, exactly as a frame that
   does not decode does today. No reflection over fields: the registry
   keys its codecs by the event's dynamic type, which is reflection over
   a type, never over a field.
- the encode side is the frame's JSON marshal of the event; the decode
  side is what the application registers, because only it can build its
  own type.

Rig's side: `store/scheduler/fleet.go` builds both ends of the fd 3 pipe
(`Fleet`, `openFleet`), so rig registers its wire types — `Verdict`,
`ToolStart`, `SwarmStatus`, `ReasoningDelta`, `Phase` and the rest
`encode.go` lists today — in that one file, under the same kind strings
the wire speaks today (`notice`, `swarm_status`, `verdict`, `phase`,
`reasoning`, `tool_start`). The frame bytes do not change; the pipe on
fd 3 carries the same bytes.

## findings the move surfaced

1. **The lift touches more than `swarm` and `tool/delegate`.** Those two
   change only their imports, as the note expects. But `Event`,
   `Snapshot`, `Notice` and `Level` leave `core`, and every consumer of
   those names changes its import or its qualified name: the provider
   streams (`<-chan Event`), the loop, the four frontends,
   `middleware/toolset`, the context policies, `store/state`'s recorder,
   the graph and decision queues, `tool/verdict`. No aliases in `core`:
   re-declaring `type Notice = evt.Notice` would leave the moved type
   living two lives in one tree. The diff is mechanical and the suite
   proves it.
2. **`broadcast`'s tests name rig's types** (the encoder's kind table,
   the swarm status payload). The module cannot import rig, so the room
   tests move as an application speaking its own events: a test-declared
   event type carries the pipe tests, and the kind-table tests become
   registry tests. Rig keeps its side where its wire types are pinned:
   the fleet wire tests.
3. **`specs/SPEC_EVT.md` moves with the engine** as history: its
   decisions 6 and 7 name rig's loop phases (2a, 2b), which are
   applications of the engine, not the engine. The module's README
   states the rule; the spec keeps rig's narrative.
4. **Rig's measured block moves at the release.** `evt/` and
   `broadcast/` leaving the tree drop the counted go lines; the
   2.18.0 release re-runs `scripts/readme-measured update`, which is
   the tree's own gate, not a module concern.
5. **`specs/FREEZE.txt` keeps its `broadcast/` line.** The gate matches
   paths a PR touches; a line whose directory left the tree is inert.
   The freeze still gates `loop/loop.go`, `loop/batch.go` and the
   kernel's priority table, which do not change in 2.18.0 beyond import
   paths.
6. **The module tags v0.1.0 before rig lifts.** Rig requires
   `github.com/mrsirg97-rgb/evt` at its tagged version — no `replace`
   directive in rig's tree, no workspace file committed. evtjs (which
   imports rig's `evt` at its pinned rig version today) and the 3.0.0
   proxy are waiting consumers; both are out of scope here.
7. **The engine's tests are external test packages** (`package
   evt_test`), so they import the engine by its module path. Moving
   them changes exactly one line per file — the import path — which is
   the diff this move's rule names. The six sources are byte-identical;
   no other change was needed.
8. **The payload's JSON carries the Go field names** (`{"Source":…,
   "Text":…, "Level":0}`): rig's `core` types never carried tags and
   the fleet pipe's bytes must not move, so `evt.Notice` moves
   without tags and rig's lift must not add any. The frame-shape test
   pins the whole frame byte for byte: origin, ok, kind, payload, and
   a heartbeat's frame with no kind.

## the hand-off

The rule's fourth thing came after the first three, and is the
one rig's delegate needed: **a cap** that gates how much work starts at
once. `NewCap(engine, workers)` — zero is no cap — hands out
`Submit(ctx, work, priority)`: a submit says whether the work started or
queued, and a started work is resolved by the submitter in its own
goroutine, right where the call returns; a queued one becomes an event
at the priority the submit named. **The cap's state is touched only by
the consumer.** The queue is a slice only the consumer's closures read
and write — no mutex, and no channel standing in for a counter; the two
counts are atomics. A submit, a release and a stop are events, so the
append that queues work and the pop that starts it can never interleave:
the same body that appends checks whether a place is free and starts the
work itself. A completion is the release: `Release` posts one event at
the priority the submit named, and its body frees the place and starts
the next queued work, which the cap resolves on its own goroutine with
the context the submitter handed in — the work may be minutes long and
the loop must not stall. `Stop` posts the same kind of event and settles
the work with a stopped context; it never starts. The submission carries
the settle: `Settled` closes when the work's resolve returns, whichever
way it went. The cap reports what it holds: `InFlight`, `Queued`, and
its own `Size`. One cap serves several submitters — the value is the
whole contract; nothing per-submitter to wire.

The first cut was a mutex and a buffered channel, and it was not safe: a
submit that fails to take a place and a release that finds the queue
empty interleave, leaving work queued with a place free and nothing to
start it — reproduced in 14 of 300,000 rounds, one release racing one
submit on a cap of 1. The rebuild moves the state behind the consumer;
the race test, one release racing one submit for 300,000 rounds, is the
proof the shape cannot lose a wakeup.

The rebuild had a second race of its own. A submit that finds a place
free takes it off the consumer, by compare-and-swap; the consumer, when
a release freed a place, read the count and then added one for the work
it was starting. Between that read and that add a submit could take the
same place: two holders on a cap of one, in 13 of 200,000 rounds. Now
both sides take a place the same way, in one compare-and-swap, and a
submit takes one off the consumer only when nothing is queued, so queued
work is never passed by a later submit. The seat test, a release racing
a submit behind a queued work for 200,000 rounds, pins both.

Checked against what the module had: none of the four existed. The
engine's `Add` answers an id, never started-or-queued; the engine counts
nothing in flight; nothing shared a cap; a pending event dropped at
`Stop` settles nothing. The hand-off borrows the engine, not the room:
the work's own posts flow through whatever engine its application runs —
rig's returns ride the room — and the hand-off only gates the starts and
owns the settle. One test per addition, as the additions landed.

## what stays in rig

`swarm` (662 lines) and `tool/delegate` (551 lines) stay: they are the
fleet, written against rig's stores. `core` keeps the agent vocabulary
and declares its types signals through the mark. The loop, the batch
and the kernel's priority table do not change beyond import paths.

## order of work

One commit per step: the design note (this file), the engine move, the
event package, the room, the wire, the tests, the docs. Then rig
2.18.0 on its own branch: first the lift, then the audit findings of
`~/.rig/audit-2.17.1.md` in its suggested-follow-ups order (H1 with its
missing table test, M-1, then the spec and doc pass), each finding
checked against the tree before it is fixed, the release commit last.
Rig keeps working at every commit.
