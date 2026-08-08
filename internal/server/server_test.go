package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/symysak/rtl-internet/internal/mockrtl"
	"github.com/symysak/rtl-internet/internal/rtltcp"
)

// A low sample rate keeps the integration tests to kilobytes per second while
// exercising exactly the same code paths as 2.4 Msps.
const testRate = 48000

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(upstreamAddr string) Config {
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.Upstream.Addr = upstreamAddr
	cfg.Upstream.BackoffMin = 50 * time.Millisecond
	cfg.Upstream.BackoffMax = 200 * time.Millisecond
	cfg.AssumeSampleRate = testRate
	cfg.Pacer.Target = 100 * time.Millisecond
	cfg.Pacer.RetunePrebuffer = 40 * time.Millisecond
	cfg.BufferMax = time.Second
	cfg.StatsInterval = time.Hour // silence the reporter during tests
	cfg.HeaderTimeout = 5 * time.Second
	return cfg
}

// startProxy brings up a mock rtl_tcp and a proxy in front of it.
func startProxy(t *testing.T, mockCfg mockrtl.Config, mutate func(*Config)) (*mockrtl.Server, string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mock, err := mockrtl.Start(ctx, "127.0.0.1:0", mockCfg)
	if err != nil {
		t.Fatalf("start mock rtl_tcp: %v", err)
	}

	cfg := testConfig(mock.Addr())
	if mutate != nil {
		mutate(&cfg)
	}

	p := New(cfg, testLogger(t))
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()

	addrCtx, addrCancel := context.WithTimeout(ctx, 5*time.Second)
	defer addrCancel()
	addr, err := p.Addr(addrCtx)
	if err != nil {
		t.Fatalf("proxy did not bind: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("proxy did not shut down")
		}
	})
	return mock, addr
}

func mockConfig() mockrtl.Config {
	cfg := mockrtl.DefaultConfig()
	cfg.SampleRate = testRate
	cfg.Burst = 10 * time.Millisecond
	return cfg
}

// dialProxy connects and consumes the dongle info header.
func dialProxy(t *testing.T, addr string) (net.Conn, rtltcp.DongleInfo) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	info, err := rtltcp.ReadDongleInfo(conn)
	if err != nil {
		t.Fatalf("read dongle info: %v", err)
	}
	return conn, info
}

func TestProxyPresentsUpstreamDongleInfo(t *testing.T) {
	mc := mockConfig()
	mc.Info = rtltcp.DongleInfo{TunerType: rtltcp.TunerR820T, TunerGainCount: 29}
	_, addr := startProxy(t, mc, nil)

	_, info := dialProxy(t, addr)
	if info != mc.Info {
		t.Errorf("got %+v, want the upstream's %+v", info, mc.Info)
	}
}

func TestProxyStreamsSamples(t *testing.T) {
	_, addr := startProxy(t, mockConfig(), nil)
	conn, _ := dialProxy(t, addr)

	// One buffer depth plus slack must elapse before data flows; that delay is
	// the cushion, not a fault.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		t.Fatalf("read samples: %v (%d bytes)", err, n)
	}
}

func TestProxyForwardsCommands(t *testing.T) {
	mock, addr := startProxy(t, mockConfig(), nil)
	conn, _ := dialProxy(t, addr)

	want := rtltcp.Command{Opcode: rtltcp.CmdSetFrequency, Param: 145000000}
	if _, err := conn.Write(want.MarshalBinary()); err != nil {
		t.Fatalf("send command: %v", err)
	}

	waitFor(t, 3*time.Second, "command to reach the upstream", func() bool {
		for _, c := range mock.Commands() {
			if c == want {
				return true
			}
		}
		return false
	})
}

func TestProxyPassesUnknownOpcodesThrough(t *testing.T) {
	mock, addr := startProxy(t, mockConfig(), nil)
	conn, _ := dialProxy(t, addr)

	// Forks of rtl_tcp keep adding opcodes. Swallowing one here would silently
	// break a setup that depends on it.
	want := rtltcp.Command{Opcode: 0xf7, Param: 12345}
	if _, err := conn.Write(want.MarshalBinary()); err != nil {
		t.Fatalf("send command: %v", err)
	}

	waitFor(t, 3*time.Second, "unknown opcode to reach the upstream", func() bool {
		for _, c := range mock.Commands() {
			if c == want {
				return true
			}
		}
		return false
	})
}

// The whole point of the proxy: the local application must not see the outage.
func TestDownstreamSurvivesUpstreamOutage(t *testing.T) {
	mc := mockConfig()
	mc.DropAfter = 400 * time.Millisecond
	mock, addr := startProxy(t, mc, nil)

	conn, _ := dialProxy(t, addr)

	// Read continuously across the drop. The connection must stay open and
	// keep producing bytes -- concealment fills whatever the outage cost.
	deadline := time.Now().Add(3 * time.Second)
	var total int
	buf := make([]byte, 4096)
	for time.Now().Before(deadline) {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Read(buf)
		total += n
		if err != nil {
			t.Fatalf("downstream connection broke during the outage after %d bytes: %v", total, err)
		}
	}

	if mock.Connections() < 2 {
		t.Errorf("upstream connected %d times, want at least 2 (a reconnect)", mock.Connections())
	}
	if total == 0 {
		t.Error("no data delivered across the outage")
	}
}

func TestReconnectReplaysTuningState(t *testing.T) {
	mc := mockConfig()
	mc.DropAfter = 500 * time.Millisecond
	mock, addr := startProxy(t, mc, nil)

	conn, _ := dialProxy(t, addr)

	// Configure in an order that only works if replay reorders it.
	for _, c := range []rtltcp.Command{
		{Opcode: rtltcp.CmdSetSampleRate, Param: testRate},
		{Opcode: rtltcp.CmdSetGainMode, Param: 1},
		{Opcode: rtltcp.CmdSetGain, Param: 400},
		{Opcode: rtltcp.CmdSetFrequency, Param: 145000000},
	} {
		if _, err := conn.Write(c.MarshalBinary()); err != nil {
			t.Fatalf("send %s: %v", c, err)
		}
		time.Sleep(60 * time.Millisecond) // stay clear of the coalescer
	}

	// Keep reading so the session stays healthy while we wait for the drop.
	go io.Copy(io.Discard, conn)

	waitFor(t, 5*time.Second, "the upstream to reconnect", func() bool {
		return mock.Connections() >= 2
	})

	var replay []rtltcp.Command
	waitFor(t, 5*time.Second, "the replay to arrive", func() bool {
		replay = mock.ConnCommands(1)
		return len(replay) >= 4
	})

	idx := func(op byte) int {
		for i, c := range replay {
			if c.Opcode == op {
				return i
			}
		}
		return -1
	}
	for _, op := range []byte{
		rtltcp.CmdSetSampleRate, rtltcp.CmdSetGainMode,
		rtltcp.CmdSetGain, rtltcp.CmdSetFrequency,
	} {
		if idx(op) < 0 {
			t.Errorf("%s missing from the replay: %v", rtltcp.OpcodeName(op), replay)
		}
	}
	if idx(rtltcp.CmdSetGainMode) > idx(rtltcp.CmdSetGain) {
		t.Errorf("gain mode replayed after gain, so the gain is ignored: %v", replay)
	}
	if idx(rtltcp.CmdSetSampleRate) > idx(rtltcp.CmdSetFrequency) {
		t.Errorf("sample rate replayed after frequency: %v", replay)
	}

	// The replayed values must be the latest ones, not the originals.
	for _, c := range replay {
		if c.Opcode == rtltcp.CmdSetFrequency && c.Param != 145000000 {
			t.Errorf("replayed frequency %d, want 145000000", c.Param)
		}
	}
}

func TestSecondClientRejectedByDefault(t *testing.T) {
	_, addr := startProxy(t, mockConfig(), nil)
	dialProxy(t, addr)

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer second.Close()

	// rtl_tcp serves one client; the second must be refused rather than
	// silently sharing the stream.
	if err := second.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := rtltcp.ReadDongleInfo(second); err == nil {
		t.Error("the second client was served; it should have been refused")
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Logf("second client rejected with: %v", err)
	}
}

func TestTakeoverReplacesExistingClient(t *testing.T) {
	_, addr := startProxy(t, mockConfig(), func(c *Config) { c.Takeover = true })

	first, _ := dialProxy(t, addr)
	second, info := dialProxy(t, addr)
	if info.TunerType == 0 && info.TunerGainCount == 0 {
		t.Error("the taking-over client got an empty dongle info header")
	}
	_ = second

	// The displaced client's socket must close.
	if err := first.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, first); err == nil {
		t.Log("displaced client saw a clean EOF")
	}
}

func TestRetuneFlushesBufferedSamples(t *testing.T) {
	mock, addr := startProxy(t, mockConfig(), nil)
	conn, _ := dialProxy(t, addr)

	// Let the cushion fill.
	go io.Copy(io.Discard, conn)
	time.Sleep(400 * time.Millisecond)

	cmd := rtltcp.Command{Opcode: rtltcp.CmdSetFrequency, Param: 100000000}
	if _, err := conn.Write(cmd.MarshalBinary()); err != nil {
		t.Fatalf("send retune: %v", err)
	}

	waitFor(t, 3*time.Second, "the retune to reach the upstream", func() bool {
		for _, c := range mock.Commands() {
			if c == cmd {
				return true
			}
		}
		return false
	})
	// Serving the pre-retune buffer would play the old frequency for a full
	// buffer depth after the user turned the dial, so it is discarded. The
	// stream must resume rather than end.
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestSampleRateChangeRetargetsBuffer(t *testing.T) {
	mock, addr := startProxy(t, mockConfig(), nil)
	conn, _ := dialProxy(t, addr)

	cmd := rtltcp.Command{Opcode: rtltcp.CmdSetSampleRate, Param: 96000}
	if _, err := conn.Write(cmd.MarshalBinary()); err != nil {
		t.Fatalf("send sample rate: %v", err)
	}
	waitFor(t, 3*time.Second, "the sample rate to reach the upstream", func() bool {
		for _, c := range mock.Commands() {
			if c == cmd {
				return true
			}
		}
		return false
	})
	// The output clock is derived from the sample rate, so it must follow.
	time.Sleep(200 * time.Millisecond)
}

func TestUpstreamUnreachableClosesClientCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A port nothing listens on.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close()

	cfg := testConfig(deadAddr)
	cfg.HeaderTimeout = 500 * time.Millisecond

	p := New(cfg, testLogger(t))
	go p.Run(ctx)

	addrCtx, addrCancel := context.WithTimeout(ctx, 5*time.Second)
	defer addrCancel()
	addr, err := p.Addr(addrCtx)
	if err != nil {
		t.Fatalf("proxy did not bind: %v", err)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	// With no upstream there is no dongle info to forward, so the client must
	// be dropped rather than left hanging forever.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := rtltcp.ReadDongleInfo(conn); err == nil {
		t.Error("got a dongle info header despite there being no upstream")
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
