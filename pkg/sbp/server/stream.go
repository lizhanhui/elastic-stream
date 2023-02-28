//nolint:unused
package server

import (
	"io"
)

// stream is the state for a single stream
type stream struct {
	cc *conn

	id uint32
	rp io.PipeReader
	wp io.PipeReader
}
