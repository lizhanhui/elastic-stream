//nolint:unused
package client

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
)

// Conn is the state of a single client connection to a server.
// TODO
type Conn struct {
	t          *Transport
	conn       net.Conn
	connClosed bool

	// readLoop goroutine fields:
	readerDone chan struct{} // closed on error
	readerErr  error         // set before readerDone is closed

	idleTimeout time.Duration // or 0 for never
	idleTimer   *time.Timer

	mu         sync.Mutex // guards following
	closing    bool
	closed     bool
	goAway     *codec.Frame              // if non-nil, the GoAwayFrame we received
	streams    map[uint32]*stream        // client-initiated
	pings      map[[8]byte]chan struct{} // in flight ping data to notification channel
	br         *bufio.Reader
	lastActive time.Time
	lastIdle   time.Time // time last idle

	// wmu is held while writing.
	// Acquire BEFORE mu when holding both, to avoid blocking mu on network writes.
	// Only acquire both at the same time when changing peer settings.
	wmu  sync.Mutex
	bw   *bufio.Writer
	fr   *codec.Framer
	werr error // first write error that has occurred
}
