# Changelog
## [0.2.0]: the payload is a signal

`evt.Event` beside `event.Event` named two different things with one
word. The engine keeps `Event`: work it runs, with an id, a priority and
a closure. What a provider streams, a frontend is told and a room's
members exchange is `evt.Signal`, in the root package beside it.

- **`evt.Signal`** (`interface{ signal() }`) replaces `event.Event`, and
  **`evt.Mark`** replaces `event.Marker` as the one door: a type is a
  signal by embedding it, and it adds no bytes to the type's JSON.
  `Snapshot`, `Notice` and `Level` move to the root with them. The
  `event` package is gone; `broadcast` imports the root only.
- **`broadcast.Message.Signal()`** replaces `Message.Event()`. The frame
  on the wire does not change.
- **the hand-off takes a place in one step.** A submit could take a place
  between the consumer reading the count and adding to it: two holders
  on a cap of one, in 13 of 200,000 rounds. Both sides now take a place
  by one compare-and-swap, and a submit takes one off the consumer only
  when nothing is queued, so queued work starts in submit order.
- **interfaces out, structs in.** `Cap`, `Submission` and
  `broadcast.Registry` are interfaces over private structs, as the
  engine, the queue and the room already were; `NewCap` and
  `NewRegistry` return them. A failed `Declare` returns a nil registry,
  and the wire's frame type is private. What stays concrete is what an
  application must build or embed: `Mark`, `Notice`, `Level`.
- CI on pull requests and main (ubuntu and macos: vet, the race suite,
  gofmt, and a check that the module requires nothing), and a release
  on a pushed tag whose notes are that version's section of this file.

Breaking for importers of 0.1.0: rename the four names above, and hold
`evt.Cap`, `evt.Submission` and `broadcast.Registry` by value, not by
pointer or literal.

## [0.1.0]: the engine, the event, the room and the hand-off

The first release, lifted from rig after 2.17.1.

- **the engine**, moved unchanged: one consumer, closures ordered by
  priority then arrival, `Queue`, `Engine`, `Scheduler`, `Clock`.
- **the event types** in `event`: `Event`, `Marker`, `Snapshot`,
  `Notice`, `Level`.
- **the room** in `broadcast`: `Message`, `Transport` (loop and pipe),
  `Member`, `Room`, `Say`, and a wire that takes a `Registry` of the
  types an application sends.
- **the hand-off**: `Cap` and `Submission`, on the engine.
