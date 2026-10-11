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
  codecs key on the signal's dynamic type.

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
