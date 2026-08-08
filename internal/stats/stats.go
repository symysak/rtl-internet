// Package stats accumulates counters and emits the periodic diagnostic line.
//
// Against a stock rtl_tcp there is no way to learn that the remote dropped
// samples: the protocol has no sequence numbers and no error channel. The one
// signal we do have is the arrival rate. If bytes show up slower than
// sample_rate*2 while the socket is connected, either the link cannot carry the
// stream or rtl_tcp's ring overflowed. That comparison is the main diagnostic
// value of this proxy, so it gets its own warning.
package stats

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/symysak/rtl-internet/internal/rtltcp"
)

// stallBucketsMS are the upper bounds, in milliseconds, of the upstream read
// gap histogram.
var stallBucketsMS = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}

const (
	// shortfallRatio is the arrival rate, relative to sample_rate*2, below
	// which an interval counts as short.
	shortfallRatio = 0.99
	// shortfallIntervals is how many consecutive short intervals are needed
	// before warning, so that jitter across an interval boundary stays quiet.
	shortfallIntervals = 2
)

// shortfallTracker decides when a run of under-rate intervals is worth
// warning about.
type shortfallTracker struct {
	consecutive int
}

// observe folds in one interval and reports whether to warn. steady is false
// for intervals that cannot be judged -- a reconnect, or a mostly
// disconnected window -- and resets the run rather than counting against it.
//
// A single interval below par is ordinary jitter landing across a boundary.
// Only a sustained shortfall is evidence, and against a stock rtl_tcp it is
// the sole evidence available that samples are being lost upstream.
func (t *shortfallTracker) observe(steady bool, ratio float64) bool {
	if !steady || ratio >= shortfallRatio {
		t.consecutive = 0
		return false
	}
	t.consecutive++
	return t.consecutive >= shortfallIntervals
}

// Gauge is the instantaneous state sampled at report time.
type Gauge struct {
	DepthBytes  int
	TargetBytes int
	SampleRate  uint32
	Connected   bool
}

// Stats holds cumulative counters. All methods are safe for concurrent use.
type Stats struct {
	upstreamBytes   atomic.Uint64
	downstreamBytes atomic.Uint64
	concealBytes    atomic.Uint64
	droppedBytes    atomic.Uint64
	reconnects      atomic.Uint64
	connectedNanos  atomic.Int64

	mu       sync.Mutex
	buckets  []uint64
	stallN   uint64
	stallMax time.Duration
}

// New returns an empty Stats.
func New() *Stats {
	return &Stats{buckets: make([]uint64, len(stallBucketsMS)+1)}
}

// AddUpstream records bytes received from the remote rtl_tcp.
func (s *Stats) AddUpstream(n int) { s.upstreamBytes.Add(uint64(n)) }

// AddDownstream records bytes handed to the local SDR application.
func (s *Stats) AddDownstream(n int) { s.downstreamBytes.Add(uint64(n)) }

// AddConceal records filler bytes emitted because the buffer ran dry.
func (s *Stats) AddConceal(n int) { s.concealBytes.Add(uint64(n)) }

// SetDropped records the ring buffer's cumulative eviction count.
func (s *Stats) SetDropped(n uint64) { s.droppedBytes.Store(n) }

// AddReconnect records one successful or attempted upstream reconnection.
func (s *Stats) AddReconnect() { s.reconnects.Add(1) }

// AddConnected records time spent with a live upstream connection, used to
// normalise the arrival rate.
func (s *Stats) AddConnected(d time.Duration) { s.connectedNanos.Add(int64(d)) }

// ObserveStall records the gap between two successive upstream reads.
func (s *Stats) ObserveStall(d time.Duration) {
	ms := d.Seconds() * 1000
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for i < len(stallBucketsMS) && ms > stallBucketsMS[i] {
		i++
	}
	s.buckets[i]++
	s.stallN++
	if d > s.stallMax {
		s.stallMax = d
	}
}

// stallSummary returns an approximate p95 and the exact max, then resets the
// histogram for the next interval.
func (s *Stats) stallSummary() (p95 time.Duration, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	max = s.stallMax
	if s.stallN > 0 {
		target := uint64(float64(s.stallN) * 0.95)
		var cum uint64
		for i, c := range s.buckets {
			cum += c
			if cum >= target {
				if i < len(stallBucketsMS) {
					p95 = time.Duration(stallBucketsMS[i] * float64(time.Millisecond))
				} else {
					p95 = max
				}
				break
			}
		}
		// The histogram reports a bucket's upper bound, which can overshoot
		// the real maximum. Reporting p95 > max reads as a broken metric.
		if p95 > max {
			p95 = max
		}
	}

	clear(s.buckets)
	s.stallN = 0
	s.stallMax = 0
	return p95, max
}

type snapshot struct {
	up, down, conceal, dropped, reconnects uint64
	connected                              time.Duration
}

func (s *Stats) read() snapshot {
	return snapshot{
		up:         s.upstreamBytes.Load(),
		down:       s.downstreamBytes.Load(),
		conceal:    s.concealBytes.Load(),
		dropped:    s.droppedBytes.Load(),
		reconnects: s.reconnects.Load(),
		connected:  time.Duration(s.connectedNanos.Load()),
	}
}

// Report periodically logs one line per interval until ctx is cancelled.
// gauge is sampled at report time for the instantaneous values.
func (s *Stats) Report(ctx context.Context, log *slog.Logger, interval time.Duration, gauge func() Gauge) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := s.read()
	last := time.Now()
	var short shortfallTracker

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cur := s.read()
			g := gauge()
			elapsed := now.Sub(last)
			last = now

			connected := cur.connected - prev.connected
			upDelta := cur.up - prev.up
			nominalBps := float64(g.SampleRate) * rtltcp.BytesPerSample

			attrs := []any{
				"depth_ms", ms(rtltcp.BytesToDuration(g.DepthBytes, g.SampleRate)),
				"target_ms", ms(rtltcp.BytesToDuration(g.TargetBytes, g.SampleRate)),
				"up_mbps", mbps(upDelta, elapsed),
				"nominal_mbps", nominalBps * 8 / 1e6,
				"down_mbps", mbps(cur.down-prev.down, elapsed),
				"conceal_ms", ms(rtltcp.BytesToDuration(int(cur.conceal-prev.conceal), g.SampleRate)),
				"dropped_bytes", cur.dropped - prev.dropped,
				"reconnects", cur.reconnects,
				"connected", g.Connected,
			}
			if p95, maxStall := s.stallSummary(); maxStall > 0 {
				attrs = append(attrs, "stall_p95_ms", ms(p95), "stall_max_ms", ms(maxStall))
			}
			log.Info("stats", attrs...)

			// Judge the arrival rate only over an interval that was connected
			// throughout and free of reconnects: a reconnect starves the
			// interval by construction, and the moments either side of it are
			// not evidence of anything.
			steady := nominalBps > 0 &&
				connected > elapsed/2 &&
				cur.reconnects == prev.reconnects
			var ratio float64
			if steady {
				ratio = float64(upDelta) / (nominalBps * connected.Seconds())
			}
			if short.observe(steady, ratio) {
				log.Warn("上流の到着レートが公称を下回り続けています（回線帯域不足、またはリモート rtl_tcp のリングバッファ溢れ）",
					"ratio", ratio,
					"intervals", short.consecutive,
					"up_mbps", mbps(upDelta, elapsed),
					"nominal_mbps", nominalBps*8/1e6,
				)
			}
			prev = cur
		}
	}
}

func mbps(bytes uint64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(bytes) * 8 / d.Seconds() / 1e6
}

func ms(d time.Duration) int64 {
	return d.Milliseconds()
}
