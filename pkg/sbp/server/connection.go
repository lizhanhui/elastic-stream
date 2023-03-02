//nolint:unused
package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	doneServing      chan struct{}          // closed when serve ends
	readFrameCh      chan frameReadResult   // written by readFrames
	wantWriteFrameCh chan frameWriteRequest // from handlers -> serve
	wroteFrameCh     chan frameWriteResult  // from writeFrameAsync -> serve, tickles more frame writes
	serveMsgCh       chan *serverMessage    // misc messages & code to send to / run on the serve loop

	// Everything following is owned by the serve loop; use serveG.Check():
	serveG              tphttp2.GoroutineLock // used to verify funcs are on serve()
	maxClientStreamID   uint32                // max ever seen from client, or 0 if there have been no client requests
	streams             map[uint32]*stream
	wScheduler          *writeScheduler // wScheduler manages frames to be written
	inFrameScheduleLoop bool            // whether we're in the scheduleFrameWrite loop
	writingFrame        bool            // started writing a frame
	writingFrameAsync   bool            // started a frame on its own goroutine but haven't heard back on wroteFrameCh
	needsFrameFlush     bool            // last frame write wasn't a flush
	inGoAway            bool            // we've started to or sent GOAWAY
	needToSendGoAway    bool            // we need to schedule a GOAWAY frame write
	shutdownTimer       *time.Timer     // nil until used
	idleTimer           *time.Timer     // nil if unused

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
		case res := <-c.wroteFrameCh:
			c.wroteFrame(res)
		case res := <-c.readFrameCh:
			// Process any written frames before reading new frames from the client since a
			// written frame could have triggered a new stream to be started.
			if c.writingFrameAsync {
				select {
				case wroteRes := <-c.wroteFrameCh:
					c.wroteFrame(wroteRes)
				default:
				}
			}
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
			c.shutdownTimer = time.AfterFunc(goAwayTimeout, func() { c.sendServeMsg(shutdownTimerMsg) })
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
		case c.readFrameCh <- frameReadResult{f, err}:
		case <-c.doneServing:
			return
		}
		if err != nil {
			return
		}
	}
}

// writeFrameFromHandler sends wr to conn.wantWriteFrameCh, but aborts
// if the connection has gone away.
//
// This must not be run from the serve goroutine itself, else it might
// deadlock writing to conn.wantWriteFrameCh (which is only mildly
// buffered and is read by serve itself). If you're on the serve
// goroutine, call writeFrame instead.
func (c *conn) writeFrameFromHandler(wr frameWriteRequest) error {
	c.serveG.CheckNotOn()
	select {
	case c.wantWriteFrameCh <- wr:
		return nil
	case <-c.doneServing:
		// Serve loop is gone.
		// Client has closed their connection to the server.
		return errors.New("client disconnected")
	}
}

// writeFrame schedules a frame to write and sends it if there's nothing
// already being written.
//
// There is no pushback here (the serve goroutine never blocks). It's
// the handlers that block, waiting for their previous frames to
// make it onto the wire
//
// If you're not on the serve goroutine, use writeFrameFromHandler instead.
func (c *conn) writeFrame(wr frameWriteRequest) {
	c.serveG.Check()
	defer c.scheduleFrameWrite()

	// We never write frames on closed streams.
	//
	// The serverConn might close an open stream while the stream's handler
	// is still running. For example, the server might close a stream when it
	// receives bad data from the client. If this happens, the handler might
	// attempt to write a frame after the stream has been closed (since the
	// handler hasn't yet been notified of the close). In this case, we simply
	// ignore the frame. The handler will notice that the stream is closed when
	// it waits for the frame to be written.
	if wr.stream.state == stateClosed {
		return
	}
	c.wScheduler.Push(wr)
}

// wroteFrame is called on the serve goroutine with the result of whatever
// happened after writing a frame.
func (c *conn) wroteFrame(res frameWriteResult) {
	c.serveG.Check()
	if !c.writingFrame {
		panic("internal error: expected to be already writing a frame")
	}
	c.writingFrame = false
	c.writingFrameAsync = false

	wr := res.wr
	if wr.endStream {
		st := wr.stream
		c.closeStream(st)
	}
	wr.replyToWriter(res.err)

	c.scheduleFrameWrite()
}

// scheduleFrameWrite tickles the frame writing scheduler.
//
// If a frame is already being written, nothing happens. This will be called again
// when the frame is done being written.
//
// If a frame isn't being written and we need to send one, the best frame
// to send is selected by conn.wScheduler.
//
// If a frame isn't being written and there's nothing else to send, we
// flush the write buffer.
func (c *conn) scheduleFrameWrite() {
	c.serveG.Check()
	if c.writingFrame || c.inFrameScheduleLoop {
		return
	}
	c.inFrameScheduleLoop = true
	for !c.writingFrameAsync {
		if c.needToSendGoAway {
			c.needToSendGoAway = false
			c.startFrameWrite(frameWriteRequest{
				// TODO goAway frame
			})
			continue
		}
		if wr, ok := c.wScheduler.Pop(); ok {
			c.startFrameWrite(wr)
			continue
		}
		if c.needsFrameFlush {
			_ = c.framer.Flush() // TODO need to handle this error?
			c.needsFrameFlush = false
			continue
		}
		break
	}
	c.inFrameScheduleLoop = false
}

// startFrameWrite starts a goroutine to write wr (in a separate
// goroutine since that might block on the network), and updates the
// serve goroutine's state about the world, updated from info in wr.
func (c *conn) startFrameWrite(wr frameWriteRequest) {
	c.serveG.Check()
	if c.writingFrame {
		panic("internal error: can only be writing one frame at a time")
	}

	st := wr.stream
	if st.state == stateClosed {
		panic(fmt.Sprintf("internal error: attempt to send frame on a closed stream: %v", wr))
	}

	c.writingFrame = true
	c.needsFrameFlush = true
	if c.framer.Available() >= wr.f.Size() {
		c.writingFrameAsync = false
		err := c.framer.WriteFrame(wr.f)
		c.wroteFrame(frameWriteResult{wr: wr, err: err})
	} else {
		c.writingFrameAsync = true
		go c.writeFrameAsync(wr)
	}
}

// writeFrameAsync runs in its own goroutine and writes a single frame
// and then reports when it's done.
// At most one goroutine can be running writeFrameAsync at a time per
// serverConn.
func (c *conn) writeFrameAsync(wr frameWriteRequest) {
	err := c.framer.WriteFrame(wr.f)
	c.wroteFrameCh <- frameWriteResult{wr: wr, err: err}
}

// processFrameFromReader processes the serve loop's read from readFrameCh from the
// frame-reading goroutine.
// processFrameFromReader returns whether the connection should be kept open.
func (c *conn) processFrameFromReader(res frameReadResult) bool {
	// TODO
	_ = res
	return false
}

func (c *conn) closeStream(st *stream) {
	_ = st
	// TODO
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
	c.scheduleFrameWrite()
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

type frameReadResult struct {
	f   codec.Frame
	err error
}

// frameWriteResult is the message passed from writeFrameAsync to the serve goroutine.
type frameWriteResult struct {
	wr  frameWriteRequest // what was written (or attempted)
	err error             // result of the writeFrame call
}
