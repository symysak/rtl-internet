// Package upstream owns the TCP connection to the remote rtl_tcp.
//
// Two rules drive the design:
//
//  1. Never stop reading. The moment we stop draining the socket, TCP pushes
//     back to the remote, rtl_tcp's own ring (500 x 16384 bytes) overflows and
//     it silently discards samples. Those losses are invisible to us, so we
//     always read and let our own ring evict -- at least that gets counted.
//  2. Hide disconnects. The local application treats a closed socket as a lost
//     device, so we reconnect underneath it and replay the tuning state.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/symysak/rtl-internet/internal/rtltcp"
	"github.com/symysak/rtl-internet/internal/state"
	"github.com/symysak/rtl-internet/internal/stats"
)

// Config configures the upstream client.
type Config struct {
	Addr        string
	RecvBuf     int
	ReadChunk   int
	DialTimeout time.Duration
	// IdleTimeout bounds how long we tolerate a connected but silent socket.
	// A live rtl_tcp streams continuously, so silence means a black hole.
	IdleTimeout time.Duration
	BackoffMin  time.Duration
	BackoffMax  time.Duration
}

// DefaultConfig returns the settings used unless overridden.
func DefaultConfig() Config {
	return Config{
		RecvBuf:     4 << 20,
		ReadChunk:   32 << 10,
		DialTimeout: 10 * time.Second,
		IdleTimeout: 10 * time.Second,
		BackoffMin:  200 * time.Millisecond,
		BackoffMax:  5 * time.Second,
	}
}

// Hooks are called from the client's own goroutine.
type Hooks struct {
	// OnConnect fires after a connection is established and the tuning state
	// has been replayed. Used to discard pre-outage data and refill.
	OnConnect func()
	// OnDongleChange fires when a reconnect lands on a different device. The
	// application's gain table is then wrong, so the session must be dropped.
	OnDongleChange func(old, new rtltcp.DongleInfo)
}

// Client maintains a reconnecting connection to a remote rtl_tcp.
type Client struct {
	cfg   Config
	sink  io.Writer
	state *state.Store
	st    *stats.Stats
	log   *slog.Logger
	hooks Hooks

	mu        sync.Mutex
	conn      net.Conn
	info      rtltcp.DongleInfo
	haveInfo  bool
	infoReady chan struct{}
}

// New returns a client that writes received IQ data to sink.
func New(cfg Config, sink io.Writer, st *state.Store, stats *stats.Stats, log *slog.Logger, hooks Hooks) *Client {
	return &Client{
		cfg:       cfg,
		sink:      sink,
		state:     st,
		st:        stats,
		log:       log,
		hooks:     hooks,
		infoReady: make(chan struct{}),
	}
}

// WaitInfo blocks until the dongle info header has been received once.
func (c *Client) WaitInfo(ctx context.Context) (rtltcp.DongleInfo, error) {
	select {
	case <-ctx.Done():
		return rtltcp.DongleInfo{}, ctx.Err()
	case <-c.infoReady:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.info, nil
	}
}

// Connected reports whether a connection is currently established.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// Send forwards a command to the remote. While disconnected it is a no-op:
// the caller has already recorded the command in the state store, so it will
// be applied by the replay that follows reconnection.
func (c *Client) Send(cmd rtltcp.Command) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(cmd.MarshalBinary()); err != nil {
		c.log.Warn("上流へのコマンド送信に失敗", "cmd", cmd.String(), "err", err)
	}
}

// Run connects and keeps reconnecting until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := c.cfg.BackoffMin

	for ctx.Err() == nil {
		conn, err := c.dial(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("上流への接続に失敗", "addr", c.cfg.Addr, "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, c.cfg.BackoffMax)
			continue
		}

		err = c.session(ctx, conn)
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, io.EOF) {
			c.log.Warn("上流との接続が切れました", "err", err)
		} else {
			c.log.Warn("上流が接続を閉じました")
		}

		// A session that produced data is evidence the endpoint is healthy,
		// so start the next backoff from the bottom.
		backoff = c.cfg.BackoffMin
		if !sleep(ctx, backoff) {
			return
		}
	}
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: c.cfg.DialTimeout, KeepAlive: 15 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Commands must go out immediately; every 5 byte frame costs the user
		// a round trip already.
		if err := tcp.SetNoDelay(true); err != nil {
			c.log.Debug("SetNoDelay に失敗", "err", err)
		}
		// A deep receive buffer lets the kernel absorb bursts while our
		// reader goroutine is scheduled out.
		if err := tcp.SetReadBuffer(c.cfg.RecvBuf); err != nil {
			c.log.Debug("SetReadBuffer に失敗", "err", err, "size", c.cfg.RecvBuf)
		}
	}
	return conn, nil
}

func (c *Client) session(ctx context.Context, conn net.Conn) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(c.cfg.DialTimeout)); err != nil {
		return err
	}
	info, err := rtltcp.ReadDongleInfo(conn)
	if err != nil {
		return err
	}

	c.mu.Lock()
	prev, hadInfo := c.info, c.haveInfo
	c.info, c.haveInfo = info, true
	c.conn = conn
	if !hadInfo {
		close(c.infoReady)
	}
	c.mu.Unlock()

	// Count re-establishments, not dial attempts: a proxy that has never
	// reached the remote has not reconnected to anything.
	if hadInfo {
		c.st.AddReconnect()
	}

	c.log.Info("上流に接続しました",
		"addr", c.cfg.Addr,
		"tuner", rtltcp.TunerName(info.TunerType),
		"gain_count", info.TunerGainCount,
	)

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
	}()

	if hadInfo && prev != info {
		// The gain table the application fetched at startup no longer
		// describes this device, so continuing would silently misapply gains.
		c.log.Warn("ドングルが差し替わりました。下流セッションを切ります",
			"old_tuner", rtltcp.TunerName(prev.TunerType),
			"new_tuner", rtltcp.TunerName(info.TunerType),
		)
		if c.hooks.OnDongleChange != nil {
			c.hooks.OnDongleChange(prev, info)
		}
	}

	if hadInfo {
		c.replay()
	}
	if c.hooks.OnConnect != nil {
		c.hooks.OnConnect()
	}

	return c.readLoop(ctx, conn)
}

func (c *Client) replay() {
	cmds := c.state.ReplaySet()
	if len(cmds) == 0 {
		return
	}
	for _, cmd := range cmds {
		c.Send(cmd)
	}
	c.log.Info("チューニング状態を再適用しました", "commands", len(cmds))
}

// readLoop drains the socket into the sink until it fails. It carries a
// trailing odd byte across reads so that only whole I/Q samples ever reach the
// ring buffer; a half sample would shift the phase of everything after it.
func (c *Client) readLoop(ctx context.Context, conn net.Conn) error {
	chunk := max(c.cfg.ReadChunk, 4096)
	buf := make([]byte, chunk+1)
	off := 0
	last := time.Now()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(c.cfg.IdleTimeout)); err != nil {
			return err
		}
		n, err := conn.Read(buf[off:])

		now := time.Now()
		gap := now.Sub(last)
		last = now
		c.st.ObserveStall(gap)
		c.st.AddConnected(gap)

		if n > 0 {
			c.st.AddUpstream(n)
			total := off + n
			whole := total - total%rtltcp.BytesPerSample
			if whole > 0 {
				if _, werr := c.sink.Write(buf[:whole]); werr != nil {
					return fmt.Errorf("sink: %w", werr)
				}
			}
			if rem := total - whole; rem > 0 {
				buf[0] = buf[whole]
				off = rem
			} else {
				off = 0
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

func nextBackoff(cur, maxBackoff time.Duration) time.Duration {
	next := cur * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
