// Package mockrtl implements a fake rtl_tcp server for tests and local
// development.
//
// It streams a known counter pattern instead of real IQ, which lets a test
// verify exactly where the stream was cut, concealed or reordered, and it can
// inject the network conditions this proxy exists to survive: added latency,
// jitter, throughput shortfall and hard disconnects.
package mockrtl

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/symysak/rtl-internet/internal/rtltcp"
)

// Config describes the fake dongle and the impairments to apply.
type Config struct {
	Info rtltcp.DongleInfo
	// SampleRate is the rate the generator produces before RateFactor.
	SampleRate uint32
	// RateFactor scales the produced rate; 0.9 simulates a link that cannot
	// carry the stream, 1.001 simulates crystal drift.
	RateFactor float64
	// Burst is the generator's write period. Larger values are burstier.
	Burst time.Duration
	// Jitter, when set, delays each burst by up to this much at random.
	Jitter time.Duration
	// DropAfter, when set, closes the connection this long after it opened.
	DropAfter time.Duration
}

// DefaultConfig returns a well-behaved 2.048 Msps dongle.
func DefaultConfig() Config {
	return Config{
		Info:       rtltcp.DongleInfo{TunerType: rtltcp.TunerR820T, TunerGainCount: 29},
		SampleRate: 2048000,
		RateFactor: 1,
		Burst:      10 * time.Millisecond,
	}
}

// Server is a listening fake rtl_tcp.
type Server struct {
	cfg Config
	ln  net.Listener

	mu sync.Mutex
	// perConn holds the commands of each connection separately, so a test can
	// assert on what a reconnected session was told rather than on the union.
	perConn [][]rtltcp.Command
}

// Start listens on addr ("127.0.0.1:0" for an ephemeral port) and serves until
// ctx is cancelled.
func Start(ctx context.Context, addr string, cfg Config) (*Server, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, ln: ln}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go s.acceptLoop(ctx)
	return s, nil
}

// Addr returns the bound address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Commands returns every command received so far across all connections.
func (s *Server) Commands() []rtltcp.Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []rtltcp.Command
	for _, c := range s.perConn {
		out = append(out, c...)
	}
	return out
}

// ConnCommands returns the commands received on the i-th connection.
func (s *Server) ConnCommands(i int) []rtltcp.Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.perConn) {
		return nil
	}
	return append([]rtltcp.Command(nil), s.perConn[i]...)
}

// Connections returns how many times a client has connected.
func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.perConn)
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.perConn = append(s.perConn, nil)
		idx := len(s.perConn) - 1
		s.mu.Unlock()
		go s.serve(ctx, idx, conn)
	}
}

func (s *Server) serve(ctx context.Context, idx int, conn net.Conn) {
	defer conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	if _, err := conn.Write(s.cfg.Info.MarshalBinary()); err != nil {
		return
	}

	go s.readCommands(idx, conn)

	if s.cfg.DropAfter > 0 {
		t := time.AfterFunc(s.cfg.DropAfter, cancel)
		defer t.Stop()
	}

	s.generate(ctx, conn)
}

func (s *Server) readCommands(idx int, conn net.Conn) {
	r := rtltcp.NewCommandReader(conn)
	for {
		cmd, err := r.Read()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.perConn[idx] = append(s.perConn[idx], cmd)
		s.mu.Unlock()
	}
}

// burst is one generated chunk together with the time it should be delivered.
type burst struct {
	data []byte
	at   time.Time
}

// generate streams a little-endian uint32 counter, repeated across the sample
// stream, so a reader can detect discontinuities exactly.
//
// Generation and delivery are separate goroutines on purpose. Sleeping for the
// jitter inside the generator would delay the next chunk too, quietly turning
// "jitter" into a throughput cut -- which is a different fault entirely, and
// one that would make the proxy look broken when it is not. Only RateFactor
// may change the average rate.
func (s *Server) generate(ctx context.Context, conn net.Conn) {
	period := s.cfg.Burst
	if period <= 0 {
		period = 10 * time.Millisecond
	}
	factor := s.cfg.RateFactor
	if factor <= 0 {
		factor = 1
	}

	bytesPerBurst := int(float64(s.cfg.SampleRate) * rtltcp.BytesPerSample * factor * period.Seconds())
	bytesPerBurst -= bytesPerBurst % 4
	if bytesPerBurst < 4 {
		bytesPerBurst = 4
	}

	ch := make(chan burst, 256)
	go s.deliver(ctx, conn, ch)

	var counter uint32
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	defer close(ch)

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			buf := make([]byte, bytesPerBurst)
			for i := 0; i+4 <= len(buf); i += 4 {
				binary.LittleEndian.PutUint32(buf[i:], counter)
				counter++
			}
			at := now
			if s.cfg.Jitter > 0 {
				at = at.Add(time.Duration(rand.Int64N(int64(s.cfg.Jitter))))
			}
			select {
			case <-ctx.Done():
				return
			case ch <- burst{data: buf, at: at}:
			}
		}
	}
}

// deliver writes bursts in order, honouring their scheduled time. TCP does not
// reorder, so neither do we: a burst never goes out before its predecessor.
func (s *Server) deliver(ctx context.Context, conn net.Conn, ch <-chan burst) {
	var lastSent time.Time
	for b := range ch {
		at := b.at
		if at.Before(lastSent) {
			at = lastSent
		}
		if d := time.Until(at); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		lastSent = time.Now()
		if _, err := conn.Write(b.data); err != nil {
			return
		}
	}
}
