# event

## What it is

The event types: an event is an interface, a snapshot is its state half,
and the one voice every background subsystem speaks with. Four names:
`Event`, `Snapshot`, `Notice`, `Level` — and the `Marker` that opens the
interface to applications. Imports nothing.

## What it includes

- `Event`: `interface{ event() }`, the method unexported, so the set of
  events stays closed: only types the module vouches for satisfy it.
- `Marker`: the one exported door — an empty struct whose `event()` an
  application carries by embedding; the method is promoted across the
  package boundary, the documented Go shape for an unexported-method
  interface. The marker moves no bytes: the JSON of a carrying type is
  the JSON of the plain one, pinned by a test.
- `Snapshot`: `interface{ Event; Snapshot() }` — a state, not a story;
  the room's transport replaces a waiting snapshot with the latest value
  instead of posting again.
- `Level`: `LevelInfo`, `LevelSuccess`, `LevelError`, with `String()`
  naming the voice's words: error when something failed or was refused,
  success when something the operator asked for completed, info
  otherwise.
- `Notice`: `Source`, `Text`, `Level` — the one voice; `broadcast.Say`
  publishes it, the level is chosen where the text is made.

## How it is consumed

An application type carries `event.Marker` and is an event; it adds a
`Snapshot()` method and is a snapshot. Inside the package `Notice`
declares the method directly; the marker exists for out-of-package
declarers — one method, one concept, one package boundary between the
two spellings.

## Gotchas

- An exported marker method was rejected: any struct with a stray
  `Event()` method — an accessor, say — would become an event by
  accident; the marker is exact.
- `Notice` carries no JSON tags on purpose: the wire's payload field
  names are the Go field names, and the frame must not move.
