//nolint:unused
package client

import (
	"context"
	"math"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// conn is the state of a single client connection to a server.
type conn struct {
	c          *Client
	conn       net.Conn
	connClosed bool

	// readLoop goroutine fields:
	readerDone chan struct{} // closed on error
	readerErr  error         // set before readerDone is closed

	idleTimeout time.Duration // or 0 for never
	idleTimer   *time.Timer

	mu              sync.Mutex // guards following
	closing         bool
	closed          bool
	goAway          *codec.Frame       // if non-nil, the GoAwayFrame we received
	streams         map[uint32]*stream // client-initiated
	streamsReserved int                // incr by reserveNewRequest; decr on RoundTrip
	nextStreamID    uint32
	pings           map[[8]byte]chan struct{} // in flight ping data to notification channel
	lastActive      time.Time

	// reqMu is a 1-element semaphore channel controlling access to sending new requests.
	// Write to reqHeaderMu to lock it, read from it to unlock.
	// Lock reqMu BEFORE mu or wmu.
	reqMu chan struct{}

	// wmu is held while writing.
	wmu  sync.Mutex
	fr   *codec.Framer
	werr error // first write error that has occurred

	lg *zap.Logger
}

func (cc *conn) RoundTrip(req protocol.OutRequest) (protocol.InResponse, error) {
	ctx := req.Context()
	s := &stream{
		cc:      cc,
		ctx:     ctx,
		abort:   make(chan struct{}),
		respEnd: make(chan struct{}),
		donec:   make(chan struct{}),
	}
	go s.doRequest(req)

	for {
		select {
		case <-s.respRcv:
			return s.res, nil
		case <-s.abort:
			// wait done
			select {
			case <-s.donec:
			case <-ctx.Done():
			}
			return nil, s.abortErr
		case <-ctx.Done():
			err := ctx.Err()
			s.abortStream(err)
			return nil, err
		}
	}
}

// readLoop runs in its own goroutine and reads and dispatches frames.
func (cc *conn) readLoop() {
	// TODO
}

func (cc *conn) reserveNewRequest() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	canTakeNewRequest := cc.goAway == nil && !cc.closed && !cc.closing && cc.nextStreamID < math.MaxInt32
	if !canTakeNewRequest {
		return false
	}
	cc.streamsReserved++
	return true
}

func (cc *conn) decrStreamReservations() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.decrStreamReservationsLocked()
}

func (cc *conn) decrStreamReservationsLocked() {
	if cc.streamsReserved > 0 {
		cc.streamsReserved--
	}
}

func (cc *conn) addStreamLocked(s *stream) {
	s.id = cc.nextStreamID
	cc.nextStreamID++
	cc.streams[s.id] = s
}

func (cc *conn) forgetStreamID(id uint32) {
	logger := cc.lg

	cc.mu.Lock()
	sLen := len(cc.streams)
	delete(cc.streams, id)
	if len(cc.streams) != sLen-1 {
		logger.Error("forgetting a stream that doesn't exist", zap.Uint32("stream-id", id))
		return
	}

	cc.lastActive = time.Now()
	if len(cc.streams) == 0 && cc.idleTimer != nil {
		cc.idleTimer.Reset(cc.idleTimeout)
	}

	if cc.goAway != nil && cc.streamsReserved == 0 && len(cc.streams) == 0 {
		logger.Info("closing conn after sending goaway", zap.Uint32("max-stream-id", cc.nextStreamID-1))
		cc.closed = true
		defer cc.closeConn()
	}

	cc.mu.Unlock()
}

// shutdown gracefully closes the connection, waiting for running streams to complete.
func (cc *conn) shutdown(ctx context.Context) error {
	// TODO send goAway and wait for all streams to be done
	_ = ctx
	return nil
}

// onIdleTimeout is called from a time.AfterFunc goroutine. It will
// only be called when we're idle, but because we're coming from a new
// goroutine, there could be a new request coming in at the same time,
// so this simply calls the synchronized closeIfIdle to shut down this
// connection. The timer could just call closeIfIdle, but this is more
// clear.
func (cc *conn) onIdleTimeout() {
	cc.closeIfIdle()
}

func (cc *conn) closeIfIdle() {
	cc.mu.Lock()
	if len(cc.streams) > 0 || cc.streamsReserved > 0 {
		cc.mu.Unlock()
		return
	}

	cc.closed = true
	nextID := cc.nextStreamID
	cc.mu.Unlock()

	cc.lg.Info("closing idle conn", zap.Uint32("max-stream-id", nextID-1))
	cc.closeConn()
}

// Close closes the client connection immediately.
//
// In-flight requests are interrupted. For a graceful shutdown, use Shutdown instead.
func (cc *conn) close() {
	err := errors.New("sbp: client connection force closed")
	cc.closeForError(err)
}

// closes the client connection immediately. In-flight requests are interrupted.
// err is sent to streams.
func (cc *conn) closeForError(err error) {
	cc.mu.Lock()
	cc.closed = true
	for _, cs := range cc.streams {
		cs.abortStreamLocked(err)
	}
	cc.mu.Unlock()
	cc.closeConn()
}

func (cc *conn) closeConn() {
	_ = cc.conn.Close()
}
