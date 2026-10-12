# broadcast

## What it is

The room: a `Room` of `Member`s, each speaking through a `Transport`,
exchanging `Message`s that carry an origin, the member's health, and a
signal. A message with no signal is a heartbeat. Nothing in the package
names an application's type: the payload is `evt.Signal`, and the one
voice is `evt.Notice`. Imports `evt` only.

## What it includes

- `message.go`: `Message` (`Origin`, `Ok`, `Signal`), `NewMessage`,
  `Heartbeat`.
- `transport.go`: `Transport` (`Send` with an ack callback, `Recv` with
  a messages callback, `Close`) and the loop transport,
  `NewLoopTransport(id, engine, priority)`: a send posts one closure at
  the transport's priority and acks on the post, since the queue is the
  durability and an event stays until the consumer runs it; the receive
  is the callback the closure resolves to on the loop's goroutine; one
  heartbeat waits per member, a second one before the first ran is not
  posted. A heartbeat, or a snapshot (`evt.Snapshot`), is a state and
  not a story: a later one replaces the waiting value instead of
  posting again. A subscriber's context ending is reported to it once
  and ends the deliveries.
- `member.go`: `Member`, a `Transport` placed in a `Room`: `Forward` to
  itself, `Publish` to the room, `Subscribe` to what arrives, `Leave`
  (closes the transport, leaves the room). Every call threads the
  caller's context. `Say(member, source, text, level...)` publishes one
  `evt.Notice`: the one voice every background subsystem speaks with;
  the level is chosen where the text is made.
- `room.go`: `Room`, built with the transport its members speak
  through (`NewRoom(id, func(origin) Transport)`): `Add`, `Remove`,
  sorted `Members`, and `Broadcast`, a fan-out to every other member
  collecting one ack each; the error names the members that missed it.
  A minted member takes the id below every id the room has ever seen,
  so a minted id never collides.
  Safe for many goroutines; the loop is still the one consumer.
- `pipe.go`: the pipe transport, `NewPipeTransport(id, rw, enc)`, for
  a member whose other end is another process: a send is one encoded
  frame per line on the writer, acked on the write; a receive reads
  frames off the reader on its own goroutine until the far end closes
  it or the context ends, and reports that end once. The parent holds
  the read end and publishes what arrives as the child's member; the
  child holds the write end and is a voice on the wire, never a member
  of a room. A frame that does not decode closes the transport: the
  rule is to fail closed.
- `encode.go`: the wire. The frame is origin, ok, kind, payload, JSON,
  one frame per line; a frame with no kind is a heartbeat. The
  `Registry` maps a kind to one signal type's two directions: the
  application declares each signal type it sends, once, by name, with
  how to encode and decode it; a duplicate kind or type refuses. The
  registry is built whole — every `Declare` returns a fresh registry,
  nothing mutates in place — and handed to the codec when it is
  constructed (`NewJSONEncoder(reg)`). An unregistered type on `Send`
  is an error at the sender, naming the type; an unknown kind on `Recv`
  is an error the receiver reports once. No reflection over fields; the
  codecs key on the signal's dynamic type. An `Operation` rides the same
  frame: the mint, the key, the parent version, the version and the
  priority travel beside the payload, so the order is agreed whatever
  the clocks say.
- `signal.go`: the protocol's own signals — `Claim`, `Release`,
  `Admission`, `Acknowledgement`, `Pinned`, `Withdrawn`, `Checkpoint`,
  `Left`, `Joined`, `Seated` — declared once in `Protocol()`'s registry.
  An application declares its own operations beside them and never
  renames these.
- `operation.go`: `Operation`, a `Message` that carries a `Mint` (tick,
  member, nonce), a key and its priority; an `Admission` also carries
  the version it branches from and the version it becomes. The order is
  priority first, then tick, then member, then nonce.
- `mint.go`: identity is time plus who you are. `MachineClock` reads the
  wall clock once at the process's start and adds the monotonic time
  elapsed since — comparable across processes, never backwards within
  one. `MemberOf` hashes an identity to a 62-bit member id (the top bit
  clear keeps it positive). The `Minter` separates a member's mints
  within one tick by nonce.
- `guard.go`: `Guard`, per-key compare-and-swap over a version chain: a
  write is admitted only from the parent it names, and two writers
  branching from the same version race one swap. It is the chain the
  fold pins through.
- `fold.go`: `Fold`, the room's state on one member, in two parts. What
  is pinned: a head and a holder set per key, moved only by `Pin` (one
  admission per version, through the guard) or by `Adopt` (a
  `Checkpoint` further along the same chain). What is not yet pinned:
  the acts still waiting, kept in the order. A pinned act leaves the
  fold, so its size is what is waiting, never the room's age. `Admits`
  is the admission rule: a key serves as many places as its row says, a
  claim takes a place, a strictly higher-priority claim enters beyond
  the places (at most one), and within one priority the claim continuing
  the conversation already served goes first. A pin that arrives ahead
  of its version waits for it; one that arrives behind is already in the
  state.
- `claim.go`: `Claimant`, a member that claims, releases and acts, in
  three rounds. The act is published and each member acknowledges it —
  a claim only once it is first in that member's order for the key — and
  it is final when every live member has. Its owner then proposes an
  `Admission` at the key's head; every member votes, and the one member
  with a better proposal for the same version refuses, so at most one
  admission per version can gather every vote. The owner pins it, tells
  the room (`Pinned`), and acts. A refused proposal is `Withdrawn` and
  made again when the key moves; the act keeps its mint and its place in
  the order. A member is gone when its connection ends: its waiting acts
  are dropped, each survivor sends the others its checkpoints and a
  `Left`, no member pins until it has heard every survivor, and what
  the gone member held is released for by whoever sees it.
- `socket.go`: the unix socket transport. `Listen` binds one socket per
  member, named by the member id and the process id; `Scan` reads the
  directory — the listing is the membership, and there is no registry
  process — and removes a file whose process is gone; `Dial` connects to
  a member's socket.

## How it is consumed

An application builds its room over the loop transport at its own
priority and is one of the members; each background subsystem holds a
member and `Say`s its notices, the level chosen where the text is made.
Across a process the parent holds the read end of a pipe, publishes what
arrives as the child's member, and registers the kinds its wire speaks;
the child holds the write end and heartbeats through it.

## Gotchas

- A send acks when posted, not when delivered: delivery is the
  consumer's turn, at the room's priority, below the operator's input.
  A busy session delays the chatter and loses nothing, since the
  application's stores hold the facts.
- The loop transport's `Recv` never blocks: it registers the callback
  and returns; the context's end is the unsubscribe.
- The loop transport takes no registry: it never crosses a process.
- The room's messages are handled in the order they arrive, all at the
  claimant's own priority. An act's priority decides its place in the
  order of admission, and its `posted` callback reaches the loop at that
  priority; it does not let one message pass another on the way in.
- A member mints nothing until `Settle`, which the application calls
  once every dial has been handed to `Subscribe`, and every dialed
  member has answered.
- `Effects.Next` runs once for each pinned act that is neither a claim
  nor a release, on every member, in the key's version order. A member
  that joins later is given heads and holders, not the acts pinned
  before it came: what those acts changed it reads from the world.
- The fold keeps no history, so it refuses an act twice only while the
  act is waiting. A transport must not deliver an act again after its
  pin; the socket and the pipe do not.
- A member that is connected and silent holds the room: every act waits
  for its acknowledgement. Only its connection ending frees the others.
