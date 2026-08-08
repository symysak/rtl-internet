// Package rtltcp implements the wire format spoken by rtl_tcp (librtlsdr).
//
// The protocol is deliberately tiny: on connect the server sends a 12 byte
// dongle info header, then streams raw interleaved 8 bit I/Q samples forever.
// The client may send 5 byte commands in the opposite direction at any time.
// There is no framing, no flow control and no error signalling.
package rtltcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// Magic is the first four bytes of the dongle info header.
const Magic = "RTL0"

// InfoSize is the size of the dongle info header in bytes.
const InfoSize = 12

// CommandSize is the size of a single command frame in bytes.
const CommandSize = 5

// BytesPerSample is the on-wire size of one complex sample: one unsigned 8 bit
// I byte plus one unsigned 8 bit Q byte.
const BytesPerSample = 2

// SilenceByte is the unsigned 8 bit midpoint. A run of these bytes decodes to
// a DC-free zero signal, which is what we emit when we have nothing to send.
const SilenceByte = 0x7f

// ErrBadMagic is returned when the dongle info header does not start with Magic.
var ErrBadMagic = errors.New("rtltcp: bad magic in dongle info header")

// Opcodes as defined by librtlsdr's include/rtl_tcp.h. Values 0x40 and above
// only exist in the librtlsdr fork; osmocom's original rtl_tcp stops at 0x0e.
const (
	CmdSetFrequency           byte = 0x01
	CmdSetSampleRate          byte = 0x02
	CmdSetGainMode            byte = 0x03
	CmdSetGain                byte = 0x04
	CmdSetFrequencyCorrection byte = 0x05
	CmdSetIFStage             byte = 0x06
	CmdSetTestMode            byte = 0x07
	CmdSetAGCMode             byte = 0x08
	CmdSetDirectSampling      byte = 0x09
	CmdSetOffsetTuning        byte = 0x0a
	CmdSetRTLCrystal          byte = 0x0b
	CmdSetTunerCrystal        byte = 0x0c
	CmdSetTunerGainByIndex    byte = 0x0d
	CmdSetBiasTee             byte = 0x0e
	CmdSetTunerBandwidth      byte = 0x40
	CmdReportI2CRegs          byte = 0x48
	CmdSetFreqHi32            byte = 0x56
)

// TunerType values reported in the dongle info header.
const (
	TunerUnknown uint32 = iota
	TunerE4000
	TunerFC0012
	TunerFC0013
	TunerFC2580
	TunerR820T
	TunerR828D
)

// TunerName returns a human readable name for a tuner type, for logging.
func TunerName(t uint32) string {
	switch t {
	case TunerE4000:
		return "E4000"
	case TunerFC0012:
		return "FC0012"
	case TunerFC0013:
		return "FC0013"
	case TunerFC2580:
		return "FC2580"
	case TunerR820T:
		return "R820T"
	case TunerR828D:
		return "R828D"
	default:
		return "unknown"
	}
}

// DongleInfo is the 12 byte header the server sends immediately on connect.
type DongleInfo struct {
	TunerType      uint32
	TunerGainCount uint32
}

// ReadDongleInfo reads and validates the 12 byte header.
func ReadDongleInfo(r io.Reader) (DongleInfo, error) {
	var buf [InfoSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return DongleInfo{}, fmt.Errorf("rtltcp: read dongle info: %w", err)
	}
	if string(buf[0:4]) != Magic {
		return DongleInfo{}, fmt.Errorf("%w: got %q", ErrBadMagic, buf[0:4])
	}
	return DongleInfo{
		TunerType:      binary.BigEndian.Uint32(buf[4:8]),
		TunerGainCount: binary.BigEndian.Uint32(buf[8:12]),
	}, nil
}

// MarshalBinary encodes the header exactly as rtl_tcp puts it on the wire.
func (d DongleInfo) MarshalBinary() []byte {
	buf := make([]byte, InfoSize)
	copy(buf[0:4], Magic)
	binary.BigEndian.PutUint32(buf[4:8], d.TunerType)
	binary.BigEndian.PutUint32(buf[8:12], d.TunerGainCount)
	return buf
}

// Command is a single 5 byte control frame.
type Command struct {
	Opcode byte
	Param  uint32
}

// MarshalBinary encodes the command frame.
func (c Command) MarshalBinary() []byte {
	buf := make([]byte, CommandSize)
	buf[0] = c.Opcode
	binary.BigEndian.PutUint32(buf[1:5], c.Param)
	return buf
}

func (c Command) String() string {
	return fmt.Sprintf("%s(%d)", OpcodeName(c.Opcode), c.Param)
}

// OpcodeName returns a human readable opcode name, for logging. Unknown
// opcodes render as their hex value rather than being hidden.
func OpcodeName(op byte) string {
	switch op {
	case CmdSetFrequency:
		return "set_frequency"
	case CmdSetSampleRate:
		return "set_sample_rate"
	case CmdSetGainMode:
		return "set_gain_mode"
	case CmdSetGain:
		return "set_gain"
	case CmdSetFrequencyCorrection:
		return "set_freq_correction"
	case CmdSetIFStage:
		return "set_if_stage"
	case CmdSetTestMode:
		return "set_test_mode"
	case CmdSetAGCMode:
		return "set_agc_mode"
	case CmdSetDirectSampling:
		return "set_direct_sampling"
	case CmdSetOffsetTuning:
		return "set_offset_tuning"
	case CmdSetRTLCrystal:
		return "set_rtl_crystal"
	case CmdSetTunerCrystal:
		return "set_tuner_crystal"
	case CmdSetTunerGainByIndex:
		return "set_gain_by_index"
	case CmdSetBiasTee:
		return "set_bias_tee"
	case CmdSetTunerBandwidth:
		return "set_tuner_bandwidth"
	case CmdReportI2CRegs:
		return "report_i2c_regs"
	case CmdSetFreqHi32:
		return "set_freq_hi32"
	default:
		return fmt.Sprintf("cmd_0x%02x", op)
	}
}

// CommandReader reads a stream of 5 byte command frames.
type CommandReader struct {
	r   io.Reader
	buf [CommandSize]byte
}

// NewCommandReader wraps r. Callers should pass a buffered reader when the
// underlying source is a socket.
func NewCommandReader(r io.Reader) *CommandReader {
	return &CommandReader{r: r}
}

// Read returns the next command. Unknown opcodes are returned verbatim so that
// callers can pass them through untouched; forks of rtl_tcp keep adding new
// ones and swallowing them here would break those setups.
func (cr *CommandReader) Read() (Command, error) {
	if _, err := io.ReadFull(cr.r, cr.buf[:]); err != nil {
		return Command{}, err
	}
	return Command{
		Opcode: cr.buf[0],
		Param:  binary.BigEndian.Uint32(cr.buf[1:5]),
	}, nil
}

// IsRetune reports whether a command invalidates already buffered samples.
// Anything buffered was captured before the retune took effect, so serving it
// would play the old frequency for one full buffer depth after the user tuned.
func IsRetune(op byte) bool {
	switch op {
	case CmdSetFrequency, CmdSetFreqHi32, CmdSetSampleRate, CmdSetDirectSampling:
		return true
	default:
		return false
	}
}

// IsCoalescable reports whether rapid repeats of an opcode may be thinned out,
// keeping only the most recent value. These are the commands a GUI emits in
// bulk while the user drags a slider.
func IsCoalescable(op byte) bool {
	switch op {
	case CmdSetFrequency, CmdSetGain, CmdSetIFStage, CmdSetTunerGainByIndex:
		return true
	default:
		return false
	}
}

// BytesToDuration converts a byte count of IQ data to its playback duration at
// the given sample rate.
func BytesToDuration(n int, sampleRate uint32) time.Duration {
	if sampleRate == 0 {
		return 0
	}
	samples := float64(n) / BytesPerSample
	return time.Duration(samples / float64(sampleRate) * float64(time.Second))
}

// DurationToBytes converts a playback duration to a byte count of IQ data at
// the given sample rate. The result is rounded down to a whole sample.
func DurationToBytes(d time.Duration, sampleRate uint32) int {
	if sampleRate == 0 || d <= 0 {
		return 0
	}
	samples := d.Seconds() * float64(sampleRate)
	return int(samples) * BytesPerSample
}
