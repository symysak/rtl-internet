// Package state remembers every control command the local application has
// issued, so that a reconnected upstream can be restored to the same tuning.
//
// Without this, a dropped TCP connection loses the frequency, gain and rate,
// and the application has no idea it needs to resend them -- it believes it is
// talking to a device that was configured once at startup.
package state

import (
	"sync"

	"github.com/symysak/rtl-internet/internal/rtltcp"
)

// replayOrder is the order in which remembered commands are re-applied.
// The ordering is not cosmetic:
//   - gain has no effect until the gain mode selects manual control
//   - the tuner must know its sample rate and crystal before a frequency is
//     meaningful, so frequency goes last
//   - direct sampling and offset tuning reconfigure the signal path entirely
//     and therefore go first
var replayOrder = []byte{
	rtltcp.CmdSetDirectSampling,
	rtltcp.CmdSetOffsetTuning,
	rtltcp.CmdSetRTLCrystal,
	rtltcp.CmdSetTunerCrystal,
	rtltcp.CmdSetSampleRate,
	rtltcp.CmdSetTunerBandwidth,
	rtltcp.CmdSetFrequencyCorrection,
	rtltcp.CmdSetAGCMode,
	rtltcp.CmdSetGainMode,
	rtltcp.CmdSetGain,
	rtltcp.CmdSetTunerGainByIndex,
	rtltcp.CmdSetIFStage,
	rtltcp.CmdSetBiasTee,
	rtltcp.CmdSetTestMode,
	rtltcp.CmdSetFreqHi32,
	rtltcp.CmdSetFrequency,
}

// Store holds the latest parameter seen for each opcode.
type Store struct {
	mu     sync.Mutex
	latest map[byte]uint32
	// arrival preserves the order in which opcodes were first seen, used to
	// replay opcodes that are not in replayOrder.
	arrival []byte
}

// New returns an empty Store.
func New() *Store {
	return &Store{latest: make(map[byte]uint32)}
}

// Record remembers a command, overwriting any previous value for its opcode.
func (s *Store) Record(c rtltcp.Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.latest[c.Opcode]; !seen {
		s.arrival = append(s.arrival, c.Opcode)
	}
	s.latest[c.Opcode] = c.Param
}

// Get returns the last value recorded for an opcode.
func (s *Store) Get(op byte) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.latest[op]
	return v, ok
}

// ReplaySet returns the recorded commands in dependency-safe order. Opcodes
// this proxy does not know about -- forks keep adding them -- are replayed
// last, in the order they were first seen.
func (s *Store) ReplaySet() []rtltcp.Command {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]rtltcp.Command, 0, len(s.latest))
	emitted := make(map[byte]bool, len(s.latest))

	for _, op := range replayOrder {
		if v, ok := s.latest[op]; ok {
			out = append(out, rtltcp.Command{Opcode: op, Param: v})
			emitted[op] = true
		}
	}
	for _, op := range s.arrival {
		if !emitted[op] {
			out = append(out, rtltcp.Command{Opcode: op, Param: s.latest[op]})
			emitted[op] = true
		}
	}
	return out
}
