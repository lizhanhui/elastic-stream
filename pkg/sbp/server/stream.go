//nolint:unused
package server

import (
	"io"
)

type streamState int

const (
	stateIdle streamState = iota
	stateOpen
	stateClosed
)

// stream is the state for a single stream
type stream struct {
	cc *conn

	id uint32
	rp io.PipeReader
	wp io.PipeReader

	state streamState
}
