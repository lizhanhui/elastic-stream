//nolint:unused
package client

import (
	"net"
	"sync"
	"time"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// conn is the state of a single client connection to a server.
// TODO
type conn struct {
	c          *Client
	conn       net.Conn
	connClosed bool

	// readLoop goroutine fields:
	readerDone chan struct{} // closed on error
	readerErr  error         // set before readerDone is closed

	idleTimeout time.Duration // or 0 for never
	idleTimer   *time.Timer

	mu           sync.Mutex // guards following
	closing      bool
	closed       bool
	goAway       *codec.Frame       // if non-nil, the GoAwayFrame we received
	streams      map[uint32]*stream // client-initiated
	nextStreamID uint32
	pings        map[[8]byte]chan struct{} // in flight ping data to notification channel
	lastActive   time.Time
	lastIdle     time.Time // time last idle

	// wmu is held while writing.
	// Acquire BEFORE mu when holding both, to avoid blocking mu on network writes.
	// Only acquire both at the same time when changing peer settings.
	wmu  sync.Mutex
	fr   *codec.Framer
	werr error // first write error that has occurred
}

func (c *conn) RoundTrip(req protocol.Request) (protocol.Response, error) {
	// TODO
	_ = req
	return nil, nil
}

// readLoop runs in its own goroutine and reads and dispatches frames.
func (c *conn) readLoop() {
	// TODO
}

func (c *conn) reserveNewRequest() bool {
	// TODO
	return true
}

// onIdleTimeout is called from a time.AfterFunc goroutine. It will
// only be called when we're idle, but because we're coming from a new
// goroutine, there could be a new request coming in at the same time,
// so this simply calls the synchronized closeIfIdle to shut down this
// connection. The timer could just call closeIfIdle, but this is more
// clear.
func (c *conn) onIdleTimeout() {
	c.closeIfIdle()
}

func (c *conn) closeIfIdle() {
	// TODO
}
