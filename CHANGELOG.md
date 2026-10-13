# Changelog
## [0.4.0]: the mesh half

0.3.0's ordering is one process's room: every member is a transport in
the same process, the wire is a pipe or a socket, and the fold pins
through a guarded version chain. 0.4.0 adds the inter-system half
beside it, unchanged: several machines agreeing on one order with no
router and no leader, where finality is a freshness horizon instead of
a quorum.

- **the clock is the chain.** `vdf_clock.go`: the VDF behind `Clock`,
  a chain of repeated squarings whose every tick carries a Wesolowski
  proof. The chain is deterministic from a seed derived by hashing the
  room's name (`GenesisSeed`), so every member that computes tick n
  produces the same output byte for byte and no member's clock can
  drift from another's without a proof refusing it. A member keeps a
  frontier — the outputs it has verified — and advances it either by
  computing the next tick or by recording a proof that verifies against
  the tick before it; the mesh's current tick is the top of that
  frontier, so a member mints at the fastest peer's pace by verifying
  (constant work per tick) rather than computing. The proof rides the
  act and the tick announcement; an unverified tick is refused, never
  trusted. Every gossip reply carries the replier's frontier with its
  proofs, so a member that is behind verifies its way up to the fastest
  peer each round; a member behind by more than the frontier holds — a
  joiner, or a machine back from sleep — trusts one entry across the
  gap (the tailnet) and verifies every tick after it. A tick whose
  predecessor is known is never trusted.
- **one order, no quorum.** `sequencer.go`: acts ordered by priority,
  then the tick, then a claim before a release, then the mint — so a
  release sorts after the claim it names even when another member
  mints it in the claim's own tick — with a horizon window. The order
  keeps the window's acts and drops the rest; a hold outlives its
  claim's place in the order. An act older than the horizon
  window is refused (`ErrStaleTick`) and never folded, an act that
  arrives late is spliced into its rightful place by unwinding the tail
  and reapplying it, and the same acts in different arrival orders
  produce the same order on every member.
- **the mesh's state.** `mesh_state.go`: who holds what, per machine —
  a claim adds a hold, a release (which carries the claim it releases)
  removes it. Both directions are undoable, which is what the reorg
  path walks.
- **the log.** `wal.go`: the acts a member carries, append-deduped,
  replayable past a tick watermark and compacted below the horizon
  window on every tick. It is what a peer pulls and what gossip
  summarizes.
- **the transport.** `net_transport.go`: TCP, length-prefixed frames,
  one wire, three verbs — a push carries acts and is acked, a pull
  carries a watermark and returns a log, a gossip carries a summary and
  returns the difference. Acks are receipts, not votes; a push the
  receiver had no room for is acked as refused.
- **the summaries.** `gossip.go`: a summary hashes a fixed number of
  buckets over each act's mint; two summaries that differ by bucket
  yield the difference, so two members reconcile by exchanging one
  summary and pulling the buckets that differ.
- **the member.** `mesh.go`: `Mesh`, the machine's one mesh member,
  constructed whole (`NewMesh(id, MeshConfig)`) with the clock,
  transport, sequencer, state and log built inside it. `Post` mints a
  claim at the current tick and folds it, and gossip carries it;
  `Release` mints a release naming the claim; either reports false
  when the act did not fold — there is no tick under it yet; the fold's holders and the live machines are read
  on the member's engine. An act is acted on once its tick is past the
  freshness horizon in the member's view, not before; acts that arrive
  early wait. A door whose dial goes unanswered for the configured
  round count is gone, and what it holds is released by whoever saw it
  hold.

## [0.3.0]: the ordering primitive

One process ordering its own work was the module's whole world. 0.3.0 adds
the other half of `broadcast`: operations ordered identically on every
member of a room with no leader, so several processes agree on who does
what next without a router between them.

- **one total order.** Priority first, then the mint's tick, then the
  member, then the nonce. The tick is the machine's: the wall clock taken
  once at the process's start plus the monotonic time elapsed since it,
  so ticks compare across processes and never go backwards within one.
  `Mint` carries the three fields; the member id is `MemberOf` over a
  hash at 62 bits.
- **the version chain with compare-and-swap.** An admission carries the
  version it branches from and the version it becomes; the `Guard` admits
  it only from the head it names, so two admissions branching from one
  version race one swap and exactly one wins.
- **the acknowledged claim.** `Claimant` publishes an act to every live
  member; each acknowledges it — a claim only once it is first in that
  member's order for the key — and it is final when every live member
  has. The owner then proposes an admission at the key's head and every
  member votes; the one member holding a better proposal for the same
  version refuses, so at most one admission per version gathers every
  vote. The owner pins it, tells the room, and acts. Nothing is acted on
  and then reordered. A refused proposal is withdrawn and made again
  when the key moves; the act keeps its mint, so claims at one priority
  enter in the order they were minted.
- **the fold is what is waiting, not what has happened.** `Fold` keeps a
  head and a holder set per key and the acts not yet pinned. A pinned act
  leaves it: request 5,000 costs what request 50 does. A member that
  joins receives each member's checkpoints and what that member still
  has waiting, never a log.
- **a member that leaves.** Its connection ending is the leave. What it
  had pinned stays; its waiting acts are dropped; each survivor sends the
  others its checkpoints and no survivor pins until it has heard them
  all, so a pin the gone member told only some of the room is known to
  all of it or to none; what it held is released for by any member, and
  the duplicates pin as nothing.
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
  `Admission`, `Acknowledgement`, `Pinned`, `Withdrawn`, `Checkpoint`,
  `Left`, `Joined` and `Seated` are declared in
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
