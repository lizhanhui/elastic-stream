//nolint:unused
package server

import (
	"context"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
	tphttp2 "github.com/AutoMQ/placement-manager/third_party/forked/golang/net/http2"
)

// conn is the state of a connection between server and client.
type conn struct {
	// Immutable:
	server *Server
	rwc    net.Conn

	ctx              context.Context
	cancelCtx        context.CancelFunc
	framer           *codec.Framer
	doneServing      chan struct{}          // closed when serverConn.serve ends
	readFrameCh      chan readFrameResult   // written by serverConn.readFrames
	wantWriteFrameCh chan frameWriteRequest // from handlers -> serve
	serveMsgCh       chan *serverMessage    // misc messages & code to send to / run on the serve loop

	// Everything following is owned by the serve loop; use serveG.Check():
	serveG            tphttp2.GoroutineLock // used to verify funcs are on serve()
	maxClientStreamID uint32                // max ever seen from client (odd), or 0 if there have been no client requests
	streams           map[uint32]*stream
	writingFrame      bool        // started writing a frame
	inGoAway          bool        // we've started to or sent GOAWAY
	needToSendGoAway  bool        // we need to schedule a GOAWAY frame write
	shutdownTimer     *time.Timer // nil until used
	idleTimer         *time.Timer // nil if unused

	// Used by startGracefulShutdown.
	shutdownOnce sync.Once

	lg *zap.Logger
}

func (c *conn) serve() {
	c.serveG.Check()
	logger := c.lg
	defer c.close()

	logger.Info("start to serve connection", zap.String("remote-addr", c.rwc.RemoteAddr().String()))

	if c.server.IdleTimeout != 0 {
		c.idleTimer = time.AfterFunc(c.server.IdleTimeout, func() { c.sendServeMsg(idleTimerMsg) })
		defer c.idleTimer.Stop()
	}

	go c.readFrames() // closed by c.rwc.Close in defer close above

	for {
		select {
		case wr := <-c.wantWriteFrameCh:
			c.writeFrame(wr)
		case res := <-c.readFrameCh:
			if !c.processFrameFromReader(res) {
				return
			}
		case msg := <-c.serveMsgCh:
			switch msg {
			case idleTimerMsg:
				logger.Info("connection is idle", zap.String("remote-addr", c.rwc.RemoteAddr().String()))
				c.goAway()
			case shutdownTimerMsg:
				logger.Info("GOAWAY close timer fired, closing connection", zap.String("remote-addr", c.rwc.RemoteAddr().String()))
			case gracefulShutdownMsg:
				logger.Info("start to shut down gracefully", zap.String("remote-addr", c.rwc.RemoteAddr().String()))
				c.goAway()
			default:
				panic("unknown timer")
			}
		}

		// Start the shutdown timer after sending a GOAWAY. When sending GOAWAY
		// with no error code (graceful shutdown), don't start the timer until
		// all open streams have been completed.
		sentGoAway := c.inGoAway && !c.needToSendGoAway && !c.writingFrame
		if sentGoAway && c.shutdownTimer == nil && len(c.streams) == 0 {
			c.shutDownIn(goAwayTimeout)
		}
	}
}

// readFrames is the loop that reads incoming frames.
// It runs on its own goroutine.
func (c *conn) readFrames() {
	c.serveG.CheckNotOn()
	for {
		f, err := c.framer.ReadFrame()
		select {
		case c.readFrameCh <- readFrameResult{f, err}:
		case <-c.doneServing:
			return
		}
		if err != nil {
			return
		}
	}
}

func (c *conn) writeFrame(wr frameWriteRequest) {
	// TODO
	_ = wr
}

// processFrameFromReader processes the serve loop's read from readFrameCh from the
// frame-reading goroutine.
// processFrameFromReader returns whether the connection should be kept open.
func (c *conn) processFrameFromReader(res readFrameResult) bool {
	// TODO
	_ = res
	return false
}

func (c *conn) close() {
	close(c.doneServing)
	if t := c.shutdownTimer; t != nil {
		t.Stop()
	}
	// TODO close streams
	_ = c.rwc.Close()
	c.cancelCtx()
	// TODO deal with panics
}

// startGracefulShutdown gracefully shuts down a connection. This
// sends GOAWAY with ErrCodeNo to tell the client we're gracefully
// shutting down. The connection isn't closed until all current
// streams are done.
//
// startGracefulShutdown returns immediately; it does not wait until
// the connection has shutdown.
func (c *conn) startGracefulShutdown() {
	c.serveG.CheckNotOn()
	c.shutdownOnce.Do(func() { c.sendServeMsg(gracefulShutdownMsg) })
}

// After sending GOAWAY with an error code (non-graceful shutdown), the
// connection will close after goAwayTimeout.
//
// If we close the connection immediately after sending GOAWAY, there may
// be unsent data in our kernel receive buffer, which will cause the kernel
// to send a TCP RST on close() instead of a FIN. This RST will abort the
// connection immediately, whether the client had received the GOAWAY.
//
// Ideally we should delay for at least 1 RTT + epsilon so the client has
// a chance to read the GOAWAY and stop sending messages. Measuring RTT
// is hard, so we approximate with 1 second. See golang.org/issue/18701.
//
// This is a var, so it can be shorter in tests, where all requests uses the
// loopback interface making the expected RTT very small.
var goAwayTimeout = 1 * time.Second

func (c *conn) goAway() {
	c.serveG.Check()
	if c.inGoAway {
		return
	}
	c.inGoAway = true
	c.needToSendGoAway = true
	// TODO write frame in high priority
	// c.writeFrame()
}

func (c *conn) shutDownIn(d time.Duration) {
	c.serveG.Check()
	c.shutdownTimer = time.AfterFunc(d, func() { c.sendServeMsg(shutdownTimerMsg) })
}

type serverMessage int

// Message values sent to serveMsgCh.
var (
	idleTimerMsg        = new(serverMessage)
	shutdownTimerMsg    = new(serverMessage)
	gracefulShutdownMsg = new(serverMessage)
)

func (c *conn) sendServeMsg(msg *serverMessage) {
	c.serveG.CheckNotOn()
	select {
	case c.serveMsgCh <- msg:
	case <-c.doneServing:
	}
}

type frameWriteRequest struct {
	f codec.Frame

	// stream is the stream on which this frame will be written.
	stream *stream

	// done, if non-nil, must be a buffered channel with space for
	// 1 message and is sent the return value from write (or an
	// earlier error) when the frame has been written.
	done chan error
}

type readFrameResult struct {
	f   codec.Frame
	err error
}
