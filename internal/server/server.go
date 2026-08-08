// Package server presents an rtl_tcp-compatible endpoint on localhost and
// wires it to the reconnecting upstream client through the jitter buffer.
//
// The local SDR application connects here believing it is talking to a plain
// rtl_tcp on the LAN. Everything this proxy does -- buffering, pacing,
// reconnection, tuning replay -- is invisible to it.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/symysak/rtl-internet/internal/pacer"
	"github.com/symysak/rtl-internet/internal/ring"
	"github.com/symysak/rtl-internet/internal/rtltcp"
	"github.com/symysak/rtl-internet/internal/state"
	"github.com/symysak/rtl-internet/internal/stats"
	"github.com/symysak/rtl-internet/internal/upstream"
)

// Config holds every knob of the proxy.
type Config struct {
	Listen   string
	Upstream upstream.Config
	Pacer    pacer.Config

	// BufferMax is the hard cap on buffered playback time. Beyond it the ring
	// evicts oldest-first rather than letting latency grow without bound.
	BufferMax time.Duration
	// AssumeSampleRate drives the output clock until the application sends
	// SET_SAMPLE_RATE.
	AssumeSampleRate uint32
	// CmdRate caps how often a single coalescable opcode reaches the wire.
	CmdRate int
	// HeaderTimeout bounds how long a newly accepted client waits for the
	// upstream dongle info before we give up on it.
	HeaderTimeout time.Duration
	StatsInterval time.Duration
	// Takeover lets a new local client displace an existing one instead of
	// being refused.
	Takeover bool
	// KeepUpstream holds the remote connection open even with no local
	// client. Off by default: an idle session would burn tens of Mbps and
	// keep the dongle claimed.
	KeepUpstream bool
}

// DefaultConfig returns the shipped defaults.
func DefaultConfig() Config {
	return Config{
		Listen:           "127.0.0.1:1234",
		Upstream:         upstream.DefaultConfig(),
		Pacer:            pacer.DefaultConfig(),
		BufferMax:        2 * time.Second,
		AssumeSampleRate: 2048000,
		CmdRate:          20,
		HeaderTimeout:    15 * time.Second,
		StatsInterval:    10 * time.Second,
	}
}

// Proxy is the whole application.
type Proxy struct {
	cfg   Config
	log   *slog.Logger
	buf   *ring.Buffer
	state *state.Store
	st    *stats.Stats
	pacer *pacer.Pacer

	mu       sync.Mutex
	up       *upstream.Client
	session  *session
	sessions uint64
	addr     string
	ready    chan struct{}
}

type session struct {
	id     uint64
	conn   net.Conn
	cancel context.CancelFunc
}

// New builds a proxy. It does not touch the network until Run.
func New(cfg Config, log *slog.Logger) *Proxy {
	st := stats.New()
	buf := ring.New(rtltcp.DurationToBytes(cfg.BufferMax, cfg.AssumeSampleRate))
	return &Proxy{
		cfg:   cfg,
		log:   log,
		buf:   buf,
		state: state.New(),
		st:    st,
		pacer: pacer.New(cfg.Pacer, buf, cfg.AssumeSampleRate, st, log),
		ready: make(chan struct{}),
	}
}

// Addr blocks until the listener is bound and returns its address. Useful when
// Listen was given port 0.
func (p *Proxy) Addr(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.ready:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.addr, nil
	}
}

// Run listens for local clients until ctx is cancelled.
func (p *Proxy) Run(ctx context.Context) error {
	go p.st.Report(ctx, p.log, p.cfg.StatsInterval, p.gauge)

	if p.cfg.KeepUpstream {
		up := p.newUpstream()
		p.mu.Lock()
		p.up = up
		p.mu.Unlock()
		go up.Run(ctx)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", p.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.cfg.Listen, err)
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	p.mu.Lock()
	p.addr = ln.Addr().String()
	p.mu.Unlock()
	close(p.ready)

	p.log.Info("待ち受け開始", "listen", ln.Addr().String(), "upstream", p.cfg.Upstream.Addr,
		"buffer", p.cfg.Pacer.Target, "buffer_max", p.cfg.BufferMax)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		go p.serve(ctx, conn)
	}
}

func (p *Proxy) gauge() stats.Gauge {
	p.mu.Lock()
	up := p.up
	p.mu.Unlock()
	p.st.SetDropped(p.buf.Dropped())
	return stats.Gauge{
		DepthBytes:  p.buf.Len(),
		TargetBytes: p.pacer.TargetBytes(),
		SampleRate:  p.pacer.SampleRate(),
		Connected:   up != nil && up.Connected(),
	}
}

func (p *Proxy) newUpstream() *upstream.Client {
	return upstream.New(p.cfg.Upstream, p.buf, p.state, p.st, p.log, upstream.Hooks{
		OnConnect: func() {
			// Whatever survived the outage is seconds stale by now, and after
			// a first connect there is nothing worth keeping either.
			p.buf.Flush()
			p.pacer.Prebuffer(p.cfg.Pacer.Target)
		},
		OnDongleChange: func(_, _ rtltcp.DongleInfo) {
			p.closeSession()
		},
	})
}

func (p *Proxy) closeSession() {
	p.mu.Lock()
	s := p.session
	p.mu.Unlock()
	if s != nil {
		s.cancel()
	}
}

func (p *Proxy) serve(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.mu.Lock()
	p.sessions++
	id := p.sessions
	prev := p.session
	if prev != nil && !p.cfg.Takeover {
		p.mu.Unlock()
		p.log.Warn("既にセッションがあるため接続を拒否しました（--takeover で置き換え可）",
			"remote", conn.RemoteAddr().String())
		return
	}
	p.session = &session{id: id, conn: conn, cancel: cancel}
	up := p.up
	if up == nil {
		up = p.newUpstream()
		p.up = up
		go up.Run(sessCtx)
	}
	p.mu.Unlock()

	if prev != nil {
		p.log.Warn("既存セッションを置き換えます", "old_session", prev.id, "new_session", id)
		prev.cancel()
		prev.conn.Close()
	}

	log := p.log.With("session", id, "remote", conn.RemoteAddr().String())
	log.Info("下流クライアントが接続しました")

	defer func() {
		p.mu.Lock()
		if p.session != nil && p.session.id == id {
			p.session = nil
			// Releasing the remote frees the dongle and stops paying for tens
			// of Mbps nobody is listening to.
			if !p.cfg.KeepUpstream {
				p.up = nil
			}
		}
		p.mu.Unlock()
		log.Info("下流クライアントが切断しました")
	}()

	infoCtx, infoCancel := context.WithTimeout(sessCtx, p.cfg.HeaderTimeout)
	info, err := up.WaitInfo(infoCtx)
	infoCancel()
	if err != nil {
		log.Error("上流のドングル情報を取得できませんでした", "err", err)
		return
	}
	if _, err := conn.Write(info.MarshalBinary()); err != nil {
		log.Error("ドングル情報の送出に失敗", "err", err)
		return
	}

	p.buf.Flush()
	p.pacer.Prebuffer(p.cfg.Pacer.Target)

	co := newCoalescer(p.cmdInterval(), func(cmd rtltcp.Command) { p.dispatch(up, cmd) })
	go co.run(sessCtx)

	go func() {
		if err := p.pacer.Run(sessCtx, &deadlineWriter{conn: conn}); err != nil && sessCtx.Err() == nil {
			log.Warn("下流への書き込みに失敗", "err", err)
		}
		cancel()
	}()

	p.readCommands(sessCtx, conn, co, log)
}

func (p *Proxy) cmdInterval() time.Duration {
	rate := max(p.cfg.CmdRate, 1)
	return time.Second / time.Duration(rate)
}

// readCommands consumes the application's control stream until it closes.
func (p *Proxy) readCommands(ctx context.Context, conn net.Conn, co *coalescer, log *slog.Logger) {
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	r := rtltcp.NewCommandReader(conn)
	for {
		cmd, err := r.Read()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				log.Debug("コマンドストリームが終了", "err", err)
			}
			return
		}
		// Record before coalescing: the replay after a reconnect must carry
		// the newest value even if this particular frame is thinned out.
		p.state.Record(cmd)
		log.Debug("コマンド受信", "cmd", cmd.String())
		if !co.submit(ctx, cmd) {
			return
		}
	}
}

// dispatch sends a command upstream and applies its local side effects.
func (p *Proxy) dispatch(up *upstream.Client, cmd rtltcp.Command) {
	up.Send(cmd)

	if !rtltcp.IsRetune(cmd.Opcode) {
		return
	}
	// Everything buffered was captured at the previous tuning. Serving it
	// would play the old frequency for one full buffer depth after the user
	// turned the dial. We cannot avoid the round trip's worth of stale
	// samples still in flight, but we can drop the rest.
	if cmd.Opcode == rtltcp.CmdSetSampleRate && cmd.Param > 0 {
		p.buf.Resize(rtltcp.DurationToBytes(p.cfg.BufferMax, cmd.Param))
		p.pacer.SetSampleRate(cmd.Param)
	}
	p.buf.Flush()
	p.pacer.Prebuffer(p.cfg.Pacer.RetunePrebuffer)
}

// deadlineWriter bounds how long a write to a wedged local client can block.
type deadlineWriter struct {
	conn net.Conn
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	if err := w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	return w.conn.Write(p)
}
