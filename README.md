# evt

A small SDK for events in Go. An engine, a signal, a room and a hand-off,
standard library only.

- **an engine**: closures run on one consumer, highest priority first,
  arrival order within a priority;
- **a signal**: an interface for what happened, told to whoever listens;
- **a room** (`broadcast`): members exchanging signals over a transport,
  in one process or across a pipe;
- **a hand-off**: a cap on how much work starts at once.

## install

    go get github.com/mrsirg97-rgb/evt@latest

```go
import (
	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)
```

Needs Go 1.26 or newer. It requires nothing else.

## example

Two members of a room on one engine. One publishes a move; the other is
told on the engine's consumer.

```go
package main

import (
	"context"
	"fmt"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

type moved struct {
	Piece string
	To    string

	evt.Mark
}

func main() {
	engine := evt.NewEngine()
	scheduler := evt.NewScheduler(engine)
	scheduler.Start()
	defer scheduler.Stop()

	room := broadcast.NewRoom("game", func(origin int64) broadcast.Transport {
		return broadcast.NewLoopTransport(origin, engine, 30)
	})
	white, black := room.Add(1), room.Add(2)

	done := make(chan struct{})
	black.Subscribe(context.Background(), func(err error, messages ...broadcast.Message) {
		for _, m := range messages {
			if mv, ok := m.Signal().(moved); ok {
				fmt.Printf("member %d moved the %s to %s\n", m.Origin(), mv.Piece, mv.To)
				close(done)
			}
		}
	})

	white.Publish(context.Background(), func(error) {},
		broadcast.NewMessage(white.Id(), true, moved{Piece: "knight", To: "f3"}))
	<-done
}
```

    member 1 moved the knight to f3

What it shows:

- `moved` is a signal because it carries `evt.Mark`. That is the whole
  declaration.
- The room's transport posts to the engine at priority 30, so the
  delivery is one more closure in the consumer's order.
- `evt.Event` is work the engine runs; `evt.Signal` is what an
  application tells. The two words never mean the same thing.

## more

Each package carries a `PACKAGE.md`. `specs/SPEC_EVT.md` is the engine's
spec and `specs/DESIGN_EVT.md` the module's design note.
[rig](https://github.com/mrsirg97-rgb/rig) is the first application.
