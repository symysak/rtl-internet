package pacer

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/symysak/rtl-internet/internal/ring"
	"github.com/symysak/rtl-internet/internal/rtltcp"
	"github.com/symysak/rtl-internet/internal/stats"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestGainClamps(t *testing.T) {
	cfg := DefaultConfig()
	target := 1000

	if got := cfg.gain(0, target); got != cfg.MinGain {
		t.Errorf("empty buffer: gain = %v, want the floor %v", got, cfg.MinGain)
	}
	if got := cfg.gain(target, target); got != 1 {
		t.Errorf("at target: gain = %v, want 1", got)
	}
	if got := cfg.gain(target*100, target); got != cfg.MaxGain {
		t.Errorf("overfull: gain = %v, want the ceiling %v", got, cfg.MaxGain)
	}
	if got := cfg.gain(100, 0); got != 1 {
		t.Errorf("zero target: gain = %v, want 1", got)
	}
}

// The controller exists to hold the cushion steady despite the dongle's
// crystal running slightly off the local clock. Left uncorrected that drift
// either grows the latency without bound or drains the buffer to nothing.
func TestControllerConvergesUnderClockDrift(t *testing.T) {
	cfg := DefaultConfig()
	const (
		rate   = 2048000
		dt     = 5 * time.Millisecond
		nomBps = float64(rate) * rtltcp.BytesPerSample
	)
	target := rtltcp.DurationToBytes(cfg.Target, rate)
	capacity := rtltcp.DurationToBytes(2*time.Second, rate)

	for _, drift := range []float64{1.0, 1.001, 0.999, 1.01, 0.99} {
		depth := 0.0
		// 60 s of simulated time is far longer than the loop needs to settle.
		for i := 0; i < 12000; i++ {
			depth += nomBps * drift * dt.Seconds()
			depth = min(depth, float64(capacity))
			out := nomBps * dt.Seconds() * cfg.gain(int(depth), target)
			depth = max(depth-out, 0)
		}

		settled := rtltcp.BytesToDuration(int(depth), rate)
		// A 1% clock error moves the equilibrium by drift/Kp = 10% of target,
		// which is the price of correcting it without ever pausing the stream.
		lo, hi := cfg.Target*8/10, cfg.Target*12/10
		if settled < lo || settled > hi {
			t.Errorf("drift %.3f: settled at %s, want within [%s, %s]", drift, settled, lo, hi)
		}
	}
}

func TestControllerRebuildsFromEmpty(t *testing.T) {
	cfg := DefaultConfig()
	const (
		rate   = 2048000
		dt     = 5 * time.Millisecond
		nomBps = float64(rate) * rtltcp.BytesPerSample
	)
	target := rtltcp.DurationToBytes(cfg.Target, rate)

	depth := 0.0
	var ticks int
	for depth < float64(target)*0.9 {
		depth += nomBps * dt.Seconds()
		depth -= nomBps * dt.Seconds() * cfg.gain(int(depth), target)
		ticks++
		if ticks > 20000 {
			t.Fatal("the buffer never refilled")
		}
	}
	// Rebuilding at up to 10% slow should take roughly 10x the target depth.
	if elapsed := time.Duration(ticks) * dt; elapsed > 30*time.Second {
		t.Errorf("refill took %s, longer than expected", elapsed)
	}
}

// countingWriter records everything written and how much of it was filler.
type countingWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	silence int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, b := range p {
		if b == rtltcp.SilenceByte {
			w.silence++
		}
	}
	w.buf.Write(p)
	return len(p), nil
}

func (w *countingWriter) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Len()
}

func (w *countingWriter) silenceCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.silence
}

func TestRunHoldsBackUntilPrebuffered(t *testing.T) {
	const rate = 48000
	cfg := DefaultConfig()
	cfg.Target = 200 * time.Millisecond
	cfg.Tick = 5 * time.Millisecond

	buf := ring.New(rtltcp.DurationToBytes(2*time.Second, rate))
	p := New(cfg, buf, rate, stats.New(), testLogger())

	w := &countingWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx, w)

	// With an empty ring nothing may be emitted, however long we wait. A relay
	// without this hold-back would have no cushion at all.
	time.Sleep(100 * time.Millisecond)
	if n := w.len(); n != 0 {
		t.Fatalf("wrote %d bytes before the buffer was primed, want 0", n)
	}

	buf.Write(make([]byte, rtltcp.DurationToBytes(cfg.Target, rate)))
	time.Sleep(150 * time.Millisecond)
	if w.len() == 0 {
		t.Error("wrote nothing after the buffer reached its target depth")
	}
}

func TestRunConcealsUnderrun(t *testing.T) {
	const rate = 48000
	cfg := DefaultConfig()
	cfg.Target = 50 * time.Millisecond
	cfg.Tick = 5 * time.Millisecond

	buf := ring.New(rtltcp.DurationToBytes(2*time.Second, rate))
	p := New(cfg, buf, rate, stats.New(), testLogger())

	// Prime just past the target, then starve it.
	buf.Write(make([]byte, rtltcp.DurationToBytes(60*time.Millisecond, rate)))

	w := &countingWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx, w)

	time.Sleep(300 * time.Millisecond)
	cancel()

	// Output must continue with filler rather than stalling: a stalled socket
	// reads to the application as a dead device.
	if w.silenceCount() == 0 {
		t.Error("no concealment bytes emitted while starved")
	}
	if w.len() == 0 {
		t.Error("output stopped entirely during the underrun")
	}
}

func TestRunPacesNearNominalRate(t *testing.T) {
	const rate = 48000
	cfg := DefaultConfig()
	cfg.Target = 100 * time.Millisecond
	cfg.Tick = 5 * time.Millisecond

	buf := ring.New(rtltcp.DurationToBytes(2*time.Second, rate))
	p := New(cfg, buf, rate, stats.New(), testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Feed continuously at the nominal rate.
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		chunk := make([]byte, rtltcp.DurationToBytes(10*time.Millisecond, rate))
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				buf.Write(chunk)
			}
		}
	}()

	w := &countingWriter{}
	go p.Run(ctx, w)

	// Let it prime, then measure a clean window.
	time.Sleep(300 * time.Millisecond)
	start := w.len()
	startedAt := time.Now()
	time.Sleep(500 * time.Millisecond)
	written := w.len() - start
	elapsed := time.Since(startedAt)
	cancel()
	<-feedDone

	expected := float64(rate) * rtltcp.BytesPerSample * elapsed.Seconds()
	ratio := float64(written) / expected
	// The controller is allowed to deviate by MinGain..MaxGain; anything
	// outside that means the output clock is wrong, not merely correcting.
	if ratio < cfg.MinGain-0.05 || ratio > cfg.MaxGain+0.05 {
		t.Errorf("wrote %d bytes in %s (%.3fx nominal), want near 1x", written, elapsed, ratio)
	}
}

func TestSetSampleRateChangesTarget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Target = time.Second
	p := New(cfg, ring.New(1024), 1000, stats.New(), testLogger())

	if got, want := p.TargetBytes(), 2000; got != want {
		t.Errorf("TargetBytes = %d, want %d", got, want)
	}
	p.SetSampleRate(2000)
	if got, want := p.TargetBytes(), 4000; got != want {
		t.Errorf("TargetBytes after rate change = %d, want %d", got, want)
	}

	// A zero rate would divide the output clock by nothing; it must be ignored.
	p.SetSampleRate(0)
	if got := p.SampleRate(); got != 2000 {
		t.Errorf("SampleRate = %d after a zero update, want 2000", got)
	}
}
