// Package ring implements the jitter buffer sitting between the upstream
// reader and the paced downstream writer.
//
// The central rule: writes never block and never fail. If the buffer is full
// we discard the oldest bytes and keep going. Blocking the writer would mean
// not draining the upstream socket, which pushes back on TCP, which overflows
// rtl_tcp's own 8 MB ring on the remote side -- and samples dropped there are
// invisible to us. Dropping here at least gets counted.
package ring

import (
	"sync"
)

// align is the sample size in bytes. Capacity and evictions are kept to a
// multiple of it so that the I/Q phase of the stream can never shift; callers
// must likewise only ever write whole samples.
const align = 2

// Buffer is a fixed capacity byte ring with oldest-first eviction.
type Buffer struct {
	mu   sync.Mutex
	buf  []byte
	r    int // read cursor
	n    int // bytes currently held
	seq  uint64
	drop uint64
}

// New returns a buffer holding at most capacity bytes.
func New(capacity int) *Buffer {
	if capacity < align {
		capacity = align
	}
	capacity -= capacity % align
	return &Buffer{buf: make([]byte, capacity)}
}

// Cap returns the capacity in bytes.
func (b *Buffer) Cap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buf)
}

// Len returns the number of bytes currently buffered.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}

// Dropped returns the total number of bytes evicted due to overflow.
func (b *Buffer) Dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.drop
}

// Seq returns a counter incremented on every Flush or Resize. A reader can
// compare it across calls to notice that the stream was discontinued.
func (b *Buffer) Seq() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seq
}

// Write appends p, evicting the oldest bytes if necessary. It always reports
// len(p) written and never returns an error.
func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	total := len(p)
	capacity := len(b.buf)

	// A write larger than the whole buffer can only leave its tail behind.
	if total >= capacity {
		b.drop += uint64(b.n) + uint64(total-capacity)
		copy(b.buf, p[total-capacity:])
		b.r = 0
		b.n = capacity
		return total, nil
	}

	if over := b.n + total - capacity; over > 0 {
		// Evict whole samples only. Dropping an odd number of bytes would
		// swap I and Q for everything that follows, mirroring the spectrum.
		if over%align != 0 {
			over++
		}
		b.r = (b.r + over) % capacity
		b.n -= over
		b.drop += uint64(over)
	}

	w := (b.r + b.n) % capacity
	written := copy(b.buf[w:], p)
	if written < total {
		copy(b.buf, p[written:])
	}
	b.n += total
	return total, nil
}

// Read copies at most len(p) bytes out and returns the count. It never blocks;
// a return of 0 means the buffer is empty, which the caller is expected to
// conceal rather than wait for.
func (b *Buffer) Read(p []byte) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := min(len(p), b.n)
	if n == 0 {
		return 0
	}
	capacity := len(b.buf)
	read := copy(p, b.buf[b.r:min(b.r+n, capacity)])
	if read < n {
		copy(p[read:n], b.buf)
	}
	b.r = (b.r + n) % capacity
	b.n -= n
	return n
}

// Flush discards everything currently buffered and bumps Seq. Used on retune,
// where every buffered byte was captured at the previous tuning.
func (b *Buffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.r = 0
	b.n = 0
	b.seq++
}

// Resize reallocates to a new capacity, discarding the contents. Called when
// the sample rate changes, since capacity is dimensioned in playback time.
func (b *Buffer) Resize(capacity int) {
	if capacity < align {
		capacity = align
	}
	capacity -= capacity % align
	b.mu.Lock()
	defer b.mu.Unlock()
	if capacity != len(b.buf) {
		b.buf = make([]byte, capacity)
	}
	b.r = 0
	b.n = 0
	b.seq++
}
