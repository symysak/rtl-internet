// Package pacer writes buffered IQ data to the local SDR application at a
// clock derived from the sample rate, rather than as fast as it arrives.
//
// This is the core of the proxy. SDR++, GQRX and friends read their socket
// greedily, so a naive relay leaves the jitter buffer permanently empty and
// every network hiccup becomes an audible gap. By deliberately holding data
// back we keep a cushion (default 500 ms) that absorbs jitter and retransmits.
//
// The cushion is maintained by a proportional controller on the output rate.
// Running 10% slow when the buffer is starved rebuilds the cushion smoothly
// instead of pausing the stream, and the same loop silently absorbs the
// long-term drift between the dongle's crystal and the local clock -- left
// alone that drift would either grow the latency without bound or drain the
// buffer to nothing.
package pacer

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/symysak/rtl-internet/internal/ring"
	"github.com/symysak/rtl-internet/internal/rtltcp"
	"github.com/symysak/rtl-internet/internal/stats"
)

// Config tunes the pacing loop.
type Config struct {
	// Target is the buffer depth the controller aims to hold.
	Target time.Duration
	// RetunePrebuffer is the depth refilled after a retune flush. It is
	// deliberately shorter than Target: a full refill would mute the stream
	// for Target on every tug of the frequency dial.
	RetunePrebuffer time.Duration
	// Tick is the control loop period.
	Tick time.Duration
	// Kp is the proportional gain. At an empty buffer the output rate becomes
	// (1 - Kp) of nominal, so 0.10 means "run 10% slow to rebuild".
	Kp float64
	// MinGain and MaxGain clamp the output rate multiplier.
	MinGain, MaxGain float64
}

// DefaultConfig returns the tuning used unless overridden.
func DefaultConfig() Config {
	return Config{
		Target:          500 * time.Millisecond,
		RetunePrebuffer: 150 * time.Millisecond,
		Tick:            5 * time.Millisecond,
		Kp:              0.10,
		MinGain:         0.90,
		MaxGain:         1.05,
	}
}

// Pacer drains a ring buffer into a writer at a controlled rate.
type Pacer struct {
	cfg Config
	buf *ring.Buffer
	st  *stats.Stats
	log *slog.Logger

	mu         sync.Mutex
	sampleRate uint32
	// prebufferTo is the depth we must reach before resuming output. Zero
	// means we are streaming.
	prebufferTo time.Duration
}

// New returns a Pacer draining buf. sampleRate is the assumed rate until the
// application sends SET_SAMPLE_RATE.
func New(cfg Config, buf *ring.Buffer, sampleRate uint32, st *stats.Stats, log *slog.Logger) *Pacer {
	return &Pacer{
		cfg:         cfg,
		buf:         buf,
		st:          st,
		log:         log,
		sampleRate:  sampleRate,
		prebufferTo: cfg.Target,
	}
}

// SampleRate returns the rate currently driving the output clock.
func (p *Pacer) SampleRate() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sampleRate
}

// SetSampleRate changes the output clock. The caller is expected to have
// flushed the ring, since buffered bytes were captured at the previous rate.
func (p *Pacer) SetSampleRate(rate uint32) {
	if rate == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sampleRate = rate
}

// Prebuffer suspends output until the buffer holds depth worth of data.
func (p *Pacer) Prebuffer(depth time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prebufferTo = depth
}

// TargetBytes returns the controller setpoint at the current sample rate.
func (p *Pacer) TargetBytes() int {
	return rtltcp.DurationToBytes(p.cfg.Target, p.SampleRate())
}

// gain returns the output rate multiplier for a given buffer occupancy.
func (c Config) gain(depth, target int) float64 {
	if target <= 0 {
		return 1
	}
	g := 1 + c.Kp*float64(depth-target)/float64(target)
	return min(max(g, c.MinGain), c.MaxGain)
}

// Run drives the loop until ctx is cancelled or w fails.
func (p *Pacer) Run(ctx context.Context, w io.Writer) error {
	ticker := time.NewTicker(p.cfg.Tick)
	defer ticker.Stop()

	// Sized for one tick at the highest rate an RTL-SDR will produce, with
	// slack for a late tick.
	out := make([]byte, 0, 64*1024)
	var carry float64
	last := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			dt := now.Sub(last)
			last = now

			p.mu.Lock()
			rate := p.sampleRate
			prebufferTo := p.prebufferTo
			p.mu.Unlock()

			depth := p.buf.Len()

			if prebufferTo > 0 {
				if depth < rtltcp.DurationToBytes(prebufferTo, rate) {
					// Emit nothing while refilling. Concealing here would
					// consume the very data we are trying to accumulate.
					continue
				}
				p.mu.Lock()
				// Only clear if nobody asked for a deeper refill meanwhile.
				if p.prebufferTo == prebufferTo {
					p.prebufferTo = 0
				}
				p.mu.Unlock()
				p.log.Debug("事前バッファリング完了", "depth_ms",
					rtltcp.BytesToDuration(depth, rate).Milliseconds())
				carry = 0
				continue
			}

			target := rtltcp.DurationToBytes(p.cfg.Target, rate)
			want := float64(rate)*rtltcp.BytesPerSample*dt.Seconds()*p.cfg.gain(depth, target) + carry
			n := int(want)
			carry = want - float64(n)
			// Whole samples only: an odd-sized write would swap I and Q for
			// the rest of the session and mirror the spectrum.
			n -= n % rtltcp.BytesPerSample
			if n <= 0 {
				continue
			}

			if cap(out) < n {
				out = make([]byte, 0, n)
			}
			out = out[:n]

			got := p.buf.Read(out)
			if got < n {
				fill := out[got:]
				for i := range fill {
					fill[i] = rtltcp.SilenceByte
				}
				p.st.AddConceal(n - got)
			}

			if _, err := w.Write(out); err != nil {
				return err
			}
			p.st.AddDownstream(n)
		}
	}
}
