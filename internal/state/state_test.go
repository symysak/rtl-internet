package state

import (
	"testing"

	"github.com/symysak/rtl-internet/internal/rtltcp"
)

func indexOf(cmds []rtltcp.Command, op byte) int {
	for i, c := range cmds {
		if c.Opcode == op {
			return i
		}
	}
	return -1
}

func TestRecordKeepsLatestValue(t *testing.T) {
	s := New()
	s.Record(rtltcp.Command{Opcode: rtltcp.CmdSetFrequency, Param: 100})
	s.Record(rtltcp.Command{Opcode: rtltcp.CmdSetFrequency, Param: 200})

	got, ok := s.Get(rtltcp.CmdSetFrequency)
	if !ok || got != 200 {
		t.Errorf("Get = (%d, %v), want (200, true)", got, ok)
	}
	if cmds := s.ReplaySet(); len(cmds) != 1 {
		t.Errorf("ReplaySet has %d entries, want 1", len(cmds))
	}
}

func TestReplayOrderRespectsDependencies(t *testing.T) {
	s := New()
	// Record in an order that would break the device if replayed verbatim.
	for _, c := range []rtltcp.Command{
		{Opcode: rtltcp.CmdSetFrequency, Param: 145000000},
		{Opcode: rtltcp.CmdSetGain, Param: 400},
		{Opcode: rtltcp.CmdSetGainMode, Param: 1},
		{Opcode: rtltcp.CmdSetSampleRate, Param: 2400000},
		{Opcode: rtltcp.CmdSetDirectSampling, Param: 0},
	} {
		s.Record(c)
	}

	cmds := s.ReplaySet()

	// Gain is ignored by the tuner until manual gain mode is selected.
	if indexOf(cmds, rtltcp.CmdSetGainMode) > indexOf(cmds, rtltcp.CmdSetGain) {
		t.Error("gain mode must be replayed before gain")
	}
	// A frequency is only meaningful once the rate and signal path are set.
	if indexOf(cmds, rtltcp.CmdSetSampleRate) > indexOf(cmds, rtltcp.CmdSetFrequency) {
		t.Error("sample rate must be replayed before frequency")
	}
	if indexOf(cmds, rtltcp.CmdSetDirectSampling) > indexOf(cmds, rtltcp.CmdSetSampleRate) {
		t.Error("direct sampling reconfigures the signal path and must come first")
	}
	if got := indexOf(cmds, rtltcp.CmdSetFrequency); got != len(cmds)-1 {
		t.Errorf("frequency is at index %d, want last (%d)", got, len(cmds)-1)
	}
}

func TestReplayIncludesUnknownOpcodesLast(t *testing.T) {
	s := New()
	s.Record(rtltcp.Command{Opcode: 0xf1, Param: 1})
	s.Record(rtltcp.Command{Opcode: rtltcp.CmdSetFrequency, Param: 2})
	s.Record(rtltcp.Command{Opcode: 0xf2, Param: 3})

	cmds := s.ReplaySet()
	if len(cmds) != 3 {
		t.Fatalf("ReplaySet has %d entries, want 3", len(cmds))
	}
	// Unknown opcodes must still be replayed -- forks add their own and a
	// dropped one silently changes the device's configuration after a
	// reconnect -- but in first-seen order, after everything we understand.
	if cmds[1].Opcode != 0xf1 || cmds[2].Opcode != 0xf2 {
		t.Errorf("unknown opcodes replayed as %+v, want 0xf1 then 0xf2", cmds[1:])
	}
}

func TestReplaySetEmpty(t *testing.T) {
	if got := New().ReplaySet(); len(got) != 0 {
		t.Errorf("ReplaySet on a fresh store returned %d entries, want 0", len(got))
	}
}

func TestConcurrentRecord(t *testing.T) {
	s := New()
	done := make(chan struct{})
	for i := range 4 {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := range 200 {
				s.Record(rtltcp.Command{Opcode: byte(i + 1), Param: uint32(j)})
				s.ReplaySet()
			}
		}(i)
	}
	for range 4 {
		<-done
	}
	if got := len(s.ReplaySet()); got != 4 {
		t.Errorf("ReplaySet has %d entries, want 4", got)
	}
}
