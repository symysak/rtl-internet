package rtltcp

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

func TestDongleInfoRoundTrip(t *testing.T) {
	want := DongleInfo{TunerType: TunerR820T, TunerGainCount: 29}
	got, err := ReadDongleInfo(bytes.NewReader(want.MarshalBinary()))
	if err != nil {
		t.Fatalf("ReadDongleInfo: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestDongleInfoWireFormat(t *testing.T) {
	// Byte-exact against what rtl_tcp puts on the wire: "RTL0" then two
	// big-endian uint32s.
	got := DongleInfo{TunerType: 5, TunerGainCount: 29}.MarshalBinary()
	want := []byte{'R', 'T', 'L', '0', 0, 0, 0, 5, 0, 0, 0, 29}
	if !bytes.Equal(got, want) {
		t.Errorf("got % x, want % x", got, want)
	}
}

func TestReadDongleInfoBadMagic(t *testing.T) {
	buf := make([]byte, InfoSize)
	copy(buf, "XXXX")
	if _, err := ReadDongleInfo(bytes.NewReader(buf)); !errors.Is(err, ErrBadMagic) {
		t.Errorf("got %v, want ErrBadMagic", err)
	}
}

func TestReadDongleInfoShort(t *testing.T) {
	if _, err := ReadDongleInfo(bytes.NewReader([]byte("RTL0"))); err == nil {
		t.Error("expected an error on a truncated header")
	}
}

func TestCommandRoundTrip(t *testing.T) {
	cmds := []Command{
		{Opcode: CmdSetFrequency, Param: 145000000},
		{Opcode: CmdSetSampleRate, Param: 2400000},
		// An opcode we know nothing about must survive untouched: forks keep
		// adding them and dropping one would break those setups.
		{Opcode: 0xf3, Param: 0xdeadbeef},
	}
	var buf bytes.Buffer
	for _, c := range cmds {
		buf.Write(c.MarshalBinary())
	}

	r := NewCommandReader(&buf)
	for i, want := range cmds {
		got, err := r.Read()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if got != want {
			t.Errorf("command %d: got %+v, want %+v", i, got, want)
		}
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("got %v, want io.EOF", err)
	}
}

func TestCommandWireFormat(t *testing.T) {
	got := Command{Opcode: CmdSetFrequency, Param: 100000000}.MarshalBinary()
	want := []byte{0x01, 0x05, 0xf5, 0xe1, 0x00}
	if !bytes.Equal(got, want) {
		t.Errorf("got % x, want % x", got, want)
	}
}

func TestPartialCommandIsNotDelivered(t *testing.T) {
	if _, err := NewCommandReader(bytes.NewReader([]byte{0x01, 0x00, 0x00})).Read(); err == nil {
		t.Error("expected an error on a partial command frame")
	}
}

func TestByteDurationConversion(t *testing.T) {
	const rate = 2048000
	for _, d := range []time.Duration{time.Millisecond, 150 * time.Millisecond, time.Second} {
		n := DurationToBytes(d, rate)
		if n%BytesPerSample != 0 {
			t.Errorf("%s: %d bytes is not a whole number of samples", d, n)
		}
		if back := BytesToDuration(n, rate); absDiff(back, d) > time.Microsecond {
			t.Errorf("%s round-tripped to %s", d, back)
		}
	}
}

func TestConversionsHandleZeroRate(t *testing.T) {
	if got := DurationToBytes(time.Second, 0); got != 0 {
		t.Errorf("DurationToBytes with a zero rate: got %d, want 0", got)
	}
	if got := BytesToDuration(1000, 0); got != 0 {
		t.Errorf("BytesToDuration with a zero rate: got %s, want 0", got)
	}
}

func TestRetuneAndCoalesceClassification(t *testing.T) {
	if !IsRetune(CmdSetFrequency) || !IsRetune(CmdSetSampleRate) {
		t.Error("frequency and sample rate changes must invalidate the buffer")
	}
	if IsRetune(CmdSetGain) {
		t.Error("a gain change does not invalidate buffered samples")
	}
	if !IsCoalescable(CmdSetFrequency) {
		t.Error("frequency must be coalescable: a slider drag emits hundreds per second")
	}
	if IsCoalescable(CmdSetSampleRate) {
		t.Error("sample rate must not be coalesced: it resizes the buffer")
	}
}

func absDiff(a, b time.Duration) time.Duration {
	if a > b {
		return a - b
	}
	return b - a
}
