package stats

import (
	"testing"
	"time"
)

func TestShortfallNeedsSustainedEvidence(t *testing.T) {
	var tr shortfallTracker

	// A single dip is jitter landing across an interval boundary.
	if tr.observe(true, 0.95) {
		t.Error("warned on the first short interval")
	}
	// Two in a row is a real shortfall.
	if !tr.observe(true, 0.95) {
		t.Error("did not warn after two consecutive short intervals")
	}
}

func TestShortfallResetsOnRecovery(t *testing.T) {
	var tr shortfallTracker
	tr.observe(true, 0.90)
	if tr.observe(true, 1.0) {
		t.Error("warned on a healthy interval")
	}
	if tr.observe(true, 0.90) {
		t.Error("warned immediately after recovery; the run should have reset")
	}
}

// A reconnect starves its interval by construction, so those intervals are not
// evidence of a bad link and must not accumulate towards a warning.
func TestShortfallIgnoresUnjudgeableIntervals(t *testing.T) {
	var tr shortfallTracker
	tr.observe(true, 0.90)
	if tr.observe(false, 0.0) {
		t.Error("warned on an interval that could not be judged")
	}
	if tr.observe(true, 0.90) {
		t.Error("an unjudgeable interval did not reset the run")
	}
}

func TestShortfallQuietAtNominal(t *testing.T) {
	var tr shortfallTracker
	for i := range 10 {
		if tr.observe(true, 1.0) {
			t.Fatalf("warned at nominal rate on interval %d", i)
		}
	}
	// Just inside the threshold must stay quiet too.
	for i := range 10 {
		if tr.observe(true, shortfallRatio) {
			t.Fatalf("warned exactly at the threshold on interval %d", i)
		}
	}
}

func TestStallSummaryNeverExceedsMax(t *testing.T) {
	s := New()
	// Values that all land in the 100ms bucket but whose true max is lower.
	for range 20 {
		s.ObserveStall(60 * time.Millisecond)
	}
	p95, maxStall := s.stallSummary()

	if maxStall != 60*time.Millisecond {
		t.Errorf("max = %s, want 60ms", maxStall)
	}
	// The histogram reports bucket upper bounds; reporting p95 > max reads as
	// a broken metric.
	if p95 > maxStall {
		t.Errorf("p95 %s exceeds max %s", p95, maxStall)
	}
}

func TestStallSummaryResetsBetweenIntervals(t *testing.T) {
	s := New()
	s.ObserveStall(500 * time.Millisecond)
	if _, maxStall := s.stallSummary(); maxStall != 500*time.Millisecond {
		t.Fatalf("max = %s, want 500ms", maxStall)
	}
	if p95, maxStall := s.stallSummary(); p95 != 0 || maxStall != 0 {
		t.Errorf("summary after reset = (%s, %s), want (0, 0)", p95, maxStall)
	}
}

func TestCountersAccumulate(t *testing.T) {
	s := New()
	s.AddUpstream(100)
	s.AddUpstream(50)
	s.AddDownstream(70)
	s.AddConceal(5)
	s.AddReconnect()
	s.SetDropped(42)
	s.AddConnected(2 * time.Second)

	got := s.read()
	if got.up != 150 || got.down != 70 || got.conceal != 5 || got.reconnects != 1 || got.dropped != 42 {
		t.Errorf("counters = %+v", got)
	}
	if got.connected != 2*time.Second {
		t.Errorf("connected = %s, want 2s", got.connected)
	}
}

func TestMbps(t *testing.T) {
	// 1 MB in one second is 8 Mbps.
	if got := mbps(1_000_000, time.Second); got != 8 {
		t.Errorf("mbps = %v, want 8", got)
	}
	if got := mbps(1000, 0); got != 0 {
		t.Errorf("mbps over a zero interval = %v, want 0", got)
	}
}
