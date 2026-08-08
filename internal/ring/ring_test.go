package ring

import (
	"bytes"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	b := New(64)
	in := []byte{1, 2, 3, 4, 5, 6}
	if n, _ := b.Write(in); n != len(in) {
		t.Fatalf("Write returned %d, want %d", n, len(in))
	}
	if got := b.Len(); got != len(in) {
		t.Errorf("Len = %d, want %d", got, len(in))
	}

	out := make([]byte, len(in))
	if n := b.Read(out); n != len(in) {
		t.Fatalf("Read returned %d, want %d", n, len(in))
	}
	if !bytes.Equal(out, in) {
		t.Errorf("got % x, want % x", out, in)
	}
	if b.Len() != 0 {
		t.Errorf("Len = %d after draining, want 0", b.Len())
	}
}

func TestReadOnEmptyReturnsZero(t *testing.T) {
	if n := New(16).Read(make([]byte, 8)); n != 0 {
		t.Errorf("Read on an empty buffer returned %d, want 0", n)
	}
}

func TestWrapAround(t *testing.T) {
	b := New(8)
	b.Write([]byte{1, 2, 3, 4, 5, 6})
	b.Read(make([]byte, 4)) // advance the read cursor
	b.Write([]byte{7, 8, 9, 10})

	out := make([]byte, 6)
	if n := b.Read(out); n != 6 {
		t.Fatalf("Read returned %d, want 6", n)
	}
	want := []byte{5, 6, 7, 8, 9, 10}
	if !bytes.Equal(out, want) {
		t.Errorf("got % x, want % x", out, want)
	}
}

func TestOverflowEvictsOldest(t *testing.T) {
	b := New(8)
	b.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	b.Write([]byte{9, 10})

	if got := b.Dropped(); got != 2 {
		t.Errorf("Dropped = %d, want 2", got)
	}
	out := make([]byte, 8)
	b.Read(out)
	want := []byte{3, 4, 5, 6, 7, 8, 9, 10}
	if !bytes.Equal(out, want) {
		t.Errorf("got % x, want % x", out, want)
	}
}

func TestWriteLargerThanCapacityKeepsTail(t *testing.T) {
	b := New(4)
	b.Write([]byte{1, 2})
	b.Write([]byte{3, 4, 5, 6, 7, 8})

	if got := b.Len(); got != 4 {
		t.Fatalf("Len = %d, want 4", got)
	}
	out := make([]byte, 4)
	b.Read(out)
	want := []byte{5, 6, 7, 8}
	if !bytes.Equal(out, want) {
		t.Errorf("got % x, want % x", out, want)
	}
	if got := b.Dropped(); got != 4 {
		t.Errorf("Dropped = %d, want 4 (2 held plus 2 of the incoming write)", got)
	}
}

// Evicting an odd number of bytes would swap I and Q for the rest of the
// session, mirroring the spectrum. Every eviction must be sample-aligned.
func TestEvictionKeepsSampleAlignment(t *testing.T) {
	b := New(10)
	b.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	// Overflowing by one byte must evict two, not one.
	b.Write([]byte{10})

	if got := b.Dropped(); got%2 != 0 {
		t.Errorf("Dropped = %d, want an even count", got)
	}
	out := make([]byte, b.Len())
	b.Read(out)
	if out[0]%2 != 0 {
		t.Errorf("buffer now starts mid-sample at value %d", out[0])
	}
}

func TestCapacityIsSampleAligned(t *testing.T) {
	for _, in := range []int{0, 1, 3, 7} {
		if got := New(in).Cap(); got%2 != 0 || got < 2 {
			t.Errorf("New(%d).Cap() = %d, want an even value of at least 2", in, got)
		}
	}
}

func TestFlushDiscardsAndBumpsSeq(t *testing.T) {
	b := New(16)
	b.Write([]byte{1, 2, 3, 4})
	before := b.Seq()

	b.Flush()

	if b.Len() != 0 {
		t.Errorf("Len = %d after Flush, want 0", b.Len())
	}
	if b.Seq() == before {
		t.Error("Seq did not change across a Flush")
	}
	// The buffer must remain usable afterwards.
	b.Write([]byte{9, 9})
	if b.Len() != 2 {
		t.Errorf("Len = %d after writing post-Flush, want 2", b.Len())
	}
}

func TestResizeChangesCapacityAndDiscards(t *testing.T) {
	b := New(16)
	b.Write([]byte{1, 2, 3, 4})

	b.Resize(64)

	if got := b.Cap(); got != 64 {
		t.Errorf("Cap = %d, want 64", got)
	}
	if b.Len() != 0 {
		t.Errorf("Len = %d after Resize, want 0", b.Len())
	}
}

func TestPartialRead(t *testing.T) {
	b := New(16)
	b.Write([]byte{1, 2, 3, 4})

	out := make([]byte, 10)
	if n := b.Read(out); n != 4 {
		t.Errorf("Read returned %d, want 4 (only 4 available)", n)
	}
}
