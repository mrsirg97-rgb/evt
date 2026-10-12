# Changelog
## [0.3.0]: the ordering primitive

One process ordering its own work was the module's whole world. 0.3.0 adds
the other half of `broadcast`: operations ordered identically on every
member of a room with no leader, so several processes agree on who does
what next without a router between them.

- **one total order, applied identically on every member.** The order is
  priority first, then the mint's tick, then the member, then the nonce.
  A late operation that belongs earlier takes its rightful place (the
  reorg); staleness past the window is refused, never reordered. The tick
  is the machine's: the wall clock taken once at the process's start plus
  the monotonic time elapsed since it, so ticks compare across processes
  and never go backwards within one. `Mint` carries the three fields; the
  member id is `MemberOf` over a hash at 62 bits.
- **the version chain with compare-and-swap.** A guarded write carries the
  parent version it branches from and the version it becomes; the `Guard`
  admits it only from the head it names. Two writers branching from the
  same version race one compare-and-swap: exactly one wins, the other is
  refused. The `GuardedSequencer` re-derives the admitted set from the
  ordered log on every apply — admission is a fold, not an incremental
  decision — which is what makes a reorg correct by construction.
- **the acknowledged claim.** `Claimant` proposes an operation to every
  live member; each member answers on the same connection once the
  operation is first in its order for the key. A claim is final when every
  live member has acknowledged it; the admission follows finality, the
  holder set follows the admission, and the request follows the holder. A
  final admission is pinned — never reordered, never evicted; a claim that
  lost its version slot is refused and mints again from the advanced head.
  A member that leaves is released for by any member; the compare-and-swap
  makes the duplicates harmless.
- **the admission rule.** A key serves as many places as its row says; a
  claim takes a place, and a strictly higher-priority claim may enter
  beyond the places — at most one — so the fair slice of whatever it waits
  for can serve it. Within one priority, the claim continuing the
  conversation already being served goes first.
- **the unix socket transport beside the pipe.** `Listen` binds one socket
  per member, named by the member id and the process id; `Scan` reads the
  directory — the listing is the membership, there is no registry process
  — and removes a file whose process is gone; `Dial` connects.
- **the protocol's signals are the module's**: `Claim`, `Release`,
  `Admission`, `Acknowledgement`, `Joined` and `Seated` are declared in
  `broadcast.Protocol()`'s registry, so an application adds only its own
  operations beside them.

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
