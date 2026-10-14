# evt

A small SDK for events in Go.

## install

```bash
go get github.com/mrsirg97-rgb/evt@latest
```

```go
import (
	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)
```

needs Go 1.26 or newer (requires nothing else).

## what it contains

- **an engine**: closures run on one consumer, highest priority first, arrival order within a priority.
- **a signal**: an interface for what happened, told to whoever listens.
- **a room** (`broadcast`): members exchanging signals over a transport, in one process or across a pipe.
- **an order** (`broadcast`): operations ordered identically on every member with no leader (priority, the machine's tick, then the member) and a guarded key admitting one writer per version.
- **a hand-off**: a cap on how much work starts at once.

## example

two members of a room on one engine. one publishes a move, the other is notified on the engine's consumer.

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

```
member 1 moved the knight to f3
```

what it shows:

- `moved` is a signal because it carries `evt.Mark`. That is the whole declaration.
- The room's transport posts to the engine at priority 30, so the delivery is one more closure in the consumer's order.
- `evt.Event` is work the engine runs, `evt.Signal` is what an application tells.

## more

Each package carries a `PACKAGE.md`. `specs/SPEC_EVT.md` is the engine's spec and `specs/DESIGN_EVT.md` the module's design note.

[rig](https://github.com/mrsirg97-rgb/rig) is the first application.
