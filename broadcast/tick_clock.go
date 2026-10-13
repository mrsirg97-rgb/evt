package broadcast

import (
	"context"
	"math/big"
	"time"

	"github.com/mrsirg97-rgb/evt"
	vdf "github.com/mrsirg97-rgb/evt/broadcast/vdf"
)

type tickClock struct {
	period time.Duration
	window int64
	voice  int
	wall   int64
	epoch  time.Time
}

func NewTickClock(period time.Duration, window int64, voice int) MeshClock {
	now := time.Now()
	return &tickClock{period: period, window: window, voice: voice, wall: now.UnixNano(), epoch: now}
}

func (c *tickClock) Step() uint64 {
	return uint64((c.wall + time.Since(c.epoch).Nanoseconds()) / c.period.Nanoseconds())
}

func (c *tickClock) Proof() *vdf.VDFProof { return nil }

func (c *tickClock) Output() *big.Int { return nil }

func (c *tickClock) Entries() []Tick { return nil }

func (c *tickClock) Record(index uint64, _ *big.Int, _ *vdf.VDFProof) bool {
	return index != 0 && int64(index) <= int64(c.Step())+c.window
}

func (c *tickClock) Trust(uint64, *big.Int, *vdf.VDFProof) bool { return false }

func (c *tickClock) Start(ctx context.Context, engine evt.Engine, onTick func(index uint64)) {
	go func() {
		ticker := time.NewTicker(c.period)
		defer ticker.Stop()
		last := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			index := c.Step()
			if index == last {
				continue
			}
			last = index
			engine.Add(evt.Func(func(context.Context) {
				if onTick != nil {
					onTick(index)
				}
			}), c.voice)
		}
	}()
}
