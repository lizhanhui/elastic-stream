package client

import (
	"io"
	"sync"
)

// stream is the state for a single stream
type stream struct {
	cc *Conn

	id uint32
	rp io.PipeReader
	wp io.PipeReader

	abortOnce sync.Once
	abort     chan struct{} // closed to signal stream should end immediately
	abortErr  error         // set if abort is closed

	donec chan struct{} // closed after the stream is in the closed state
}
