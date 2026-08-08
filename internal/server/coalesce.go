package server

import (
	"context"
	"time"

	"github.com/symysak/rtl-internet/internal/rtltcp"
)

// coalescer thins out bursts of the same command.
//
// Dragging a frequency slider in SDR++ emits hundreds of SET_FREQUENCY frames
// per second. Forwarding all of them wastes a round trip each and makes the
// remote tuner chase stale values. We send the first one immediately so single
// commands stay responsive, then at most one per interval, always carrying the
// most recent value.
type coalescer struct {
	interval time.Duration
	send     func(rtltcp.Command)
	in       chan rtltcp.Command
}

func newCoalescer(interval time.Duration, send func(rtltcp.Command)) *coalescer {
	return &coalescer{
		interval: interval,
		send:     send,
		in:       make(chan rtltcp.Command, 64),
	}
}

// submit hands a command to the coalescer. It reports false once ctx is done.
func (c *coalescer) submit(ctx context.Context, cmd rtltcp.Command) bool {
	select {
	case <-ctx.Done():
		return false
	case c.in <- cmd:
		return true
	}
}

func (c *coalescer) run(ctx context.Context) {
	pending := make(map[byte]uint32)
	lastSent := make(map[byte]time.Time)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case cmd := <-c.in:
			if !rtltcp.IsCoalescable(cmd.Opcode) {
				c.send(cmd)
				continue
			}
			now := time.Now()
			if last, ok := lastSent[cmd.Opcode]; !ok || now.Sub(last) >= c.interval {
				c.send(cmd)
				lastSent[cmd.Opcode] = now
				delete(pending, cmd.Opcode)
				continue
			}
			pending[cmd.Opcode] = cmd.Param

		case now := <-ticker.C:
			for op, param := range pending {
				c.send(rtltcp.Command{Opcode: op, Param: param})
				lastSent[op] = now
				delete(pending, op)
			}
		}
	}
}
