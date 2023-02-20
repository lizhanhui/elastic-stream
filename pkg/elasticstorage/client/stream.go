package client

import (
	"sync"
)

// clientStream is the state for a single stream
type stream struct {
	cc *Conn

	ID uint32

	abortOnce sync.Once
	abort     chan struct{} // closed to signal stream should end immediately
	abortErr  error         // set if abort is closed

	donec chan struct{} // closed after the stream is in the closed state
}
