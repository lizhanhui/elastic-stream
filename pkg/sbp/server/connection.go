package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/util/logutil"
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
	serveG              tphttp2.GoroutineLock // used to verify func is on serve()
	maxClientStreamID   uint32                // max ever seen from client, or 0 if there have been no client requests
	streams             map[uint32]*stream
	wScheduler          *writeScheduler // wScheduler manages frames to be written
	inFrameScheduleLoop bool            // whether we're in the scheduleFrameWrite loop
	writingFrame        bool            // started writing a frame
	writingFrameAsync   bool            // started a frame on its own goroutine but haven't heard back on wroteFrameCh
	needsFrameFlush     bool            // last frame write wasn't a flush
	inGoAway            bool            // we've started to or sent GOAWAY
	needToSendGoAway    bool            // we need to schedule a GOAWAY frame write
	isGoAwayResponse    bool            // we started a GOAWAY response rather than a request
	shutdownTimer       *time.Timer     // nil until used
	idleTimeout         time.Duration   // zero if disabled
	idleTimer           *time.Timer     // nil if unused

	// Used by startGracefulShutdown.
	shutdownOnce sync.Once

	lg *zap.Logger
}

func (c *conn) serve() {
	c.serveG.Check()
	logger := c.lg
	defer logutil.LogPanic(logger)
	defer c.close()

	logger.Info("start to serve connection")

	if c.idleTimeout != 0 {
		c.idleTimer = time.AfterFunc(c.idleTimeout, func() { c.sendServeMsg(idleTimerMsg) })
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
				logger.Info("connection is idle")
				c.goAway(false)
			case shutdownTimerMsg:
				logger.Info("GOAWAY close timer fired, closing connection")
			case gracefulShutdownMsg:
				logger.Info("start to shut down gracefully")
				c.goAway(false)
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
		f, free, err := c.framer.ReadFrame()
		select {
		case c.readFrameCh <- frameReadResult{f, free, err}:
		case <-c.doneServing:
			return
		}
		if err != nil {
			// TODO check errors, skip stream errors
			return
		}
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
		// TODO still need this now?
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

	if res.wr.free != nil {
		res.wr.free()
	}

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
// If a frame isn't being written, and we need to send one, the best frame
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
			var goAwayStream *stream
			if c.isGoAwayResponse {
				goAwayStream = c.streams[c.maxClientStreamID]
			} else {
				goAwayStream = c.newStream(c.maxClientStreamID + 1)
			}
			c.startFrameWrite(frameWriteRequest{
				f:         codec.NewGoAwayFrame(goAwayStream.id, c.isGoAwayResponse),
				stream:    goAwayStream,
				endStream: true,
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
	logger := c.lg
	c.serveG.Check()
	if res.free != nil {
		defer res.free()
	}

	err := res.err
	if err != nil {
		clientGone := err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "use of closed network connection")
		if clientGone {
			return false
		}
	} else {
		f := res.f
		if logger.Core().Enabled(zapcore.DebugLevel) {
			logger.Debug("server read frame", zap.String("frame", f.Summarize()))
		}

		err = c.processFrame(f)
		if err == nil {
			return true
		}
	}
	if res.err != nil {
		logger.Error("failed to read frame from client connection", zap.Error(err))
	} else {
		logger.Error("failed to process frame", zap.Error(err))
	}
	// TODO switch error type
	c.goAway(false)
	return true
}

func (c *conn) processFrame(f codec.Frame) error {
	logger := c.lg
	c.serveG.Check()

	streamID := f.Base().StreamID

	// Discard frames for streams initiated after the identified last stream sent in a GOAWAY
	if c.inGoAway && streamID > c.maxClientStreamID {
		logger.Warn("server ignoring frame for stream initiated after GOAWAY", zap.String("frame", f.Info()))
		return nil
	}

	// ignore response frames
	if f.IsResponse() {
		if _, ok := f.(*codec.GoAwayFrame); !ok {
			logger.Warn("server ignoring response frame", zap.String("frame", f.Info()))
		}
		return nil
	}

	if streamID <= c.maxClientStreamID {
		logger.Error("server received a frame with an ID that has decreased", zap.String("frame", f.Info()))
		return errors.New("decreased stream ID")
	}

	st := c.newStream(streamID)

	switch f := f.(type) {
	case *codec.PingFrame:
		return c.processPing(f, st)
	case *codec.GoAwayFrame:
		return c.processGoAway(f, st)
	case *codec.HeartbeatFrame:
		return c.processHeartbeat(f, st)
	case *codec.DataFrame:
		return c.processDataFrame(f, st)
	default:
		logger.Warn("server ignoring unknown type frame", zap.String("frame", f.Info()))
		return nil
	}
}

func (c *conn) processPing(f *codec.PingFrame, st *stream) error {
	c.serveG.Check()
	outFrame, free := codec.NewPingFrameResp(f)
	c.writeFrame(frameWriteRequest{
		f:         outFrame,
		free:      free,
		stream:    st,
		endStream: true,
	})
	return nil
}

func (c *conn) processGoAway(f *codec.GoAwayFrame, _ *stream) error {
	logger := c.lg
	c.serveG.Check()
	logger.Info("received GOAWAY frame, starting graceful shutdown", zap.Uint32("max-stream-id", f.StreamID))
	c.goAway(true)
	return nil
}

func (c *conn) processHeartbeat(f *codec.HeartbeatFrame, st *stream) error {
	c.serveG.Check()
	outFrame, free := codec.NewHeartBeatFrameResp(f)
	c.writeFrame(frameWriteRequest{
		f:         outFrame,
		free:      free,
		stream:    st,
		endStream: true,
	})
	return nil
}

func (c *conn) processDataFrame(f *codec.DataFrame, st *stream) error {
	c.serveG.Check()
	_ = f
	_ = st
	if c.idleTimer != nil {
		c.idleTimer.Stop()
	}

	action := GetAction(f.OpCode)
	req := action.newReq()
	err := req.Unmarshal(f.HeaderFmt, f.Header)
	if err != nil {
		return errors.Wrap(err, "unmarshal frame header")
	}
	// TODO if there are too many handlers running, put the request into a priority queue (or put important requests into a priority queue)
	go c.runHandlerAndWrite(f.Context(), st, func() protocol.Response { return action.act(c.server.handler, req) })
	return nil
}

var errChanPool = sync.Pool{
	New: func() interface{} { return make(chan error, 1) },
}

func (c *conn) runHandlerAndWrite(frameCtx *codec.DataFrameContext, st *stream, act func() protocol.Response) {
	logger := c.lg
	c.serveG.CheckNotOn()

	resp := c.runHandler(act)
	header, err := resp.Marshal(frameCtx.HeaderFmt)
	if err != nil {
		// TODO error is always nil now, handle it later
		logger.Error("failed to marshal response header", zap.Error(err))
		return
	}

	errCh := errChanPool.Get().(chan error)
	var free func()
	if header != nil {
		free = func() { mcache.Free(header) }
	}

	err = c.writeFrameFromHandler(frameWriteRequest{
		f:         codec.NewDataFrameResp(frameCtx, header, nil, resp.IsEnd()),
		free:      free,
		stream:    st,
		done:      errCh,
		endStream: resp.IsEnd(),
	})
	if err != nil {
		// TODO error is "connection closed", handle it later
		logger.Error("failed to schedule to write response frame", zap.Error(err))
		return
	}
	select {
	case err = <-errCh:
	case <-c.doneServing:
		logger.Warn("failed to write response frame, connection closed")
		return
	}
	errChanPool.Put(errCh)
	if err != nil {
		// TODO error is "frame too large" or "connection write failed", handle it later
		logger.Error("failed to write response frame", zap.Error(err))
	}
}

func (c *conn) runHandler(act func() protocol.Response) (resp protocol.Response) {
	logger := c.lg
	didPanic := true
	defer func() {
		if didPanic {
			e := recover()
			resp = &protocol.SystemErrorResponse{
				ErrorCode:    protocol.InternalError,
				ErrorMessage: "handler panic",
			}
			if e != nil {
				logger.Error("panic serving", zap.Reflect("panic", e), zap.Stack("stack"))
			}
		}
	}()
	resp = act()
	didPanic = false
	return
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

func (c *conn) newStream(id uint32) *stream {
	c.serveG.Check()
	st := &stream{
		cc:    c,
		id:    id,
		state: stateOpen,
	}
	c.streams[id] = st
	c.maxClientStreamID = id
	return st
}

func (c *conn) closeStream(st *stream) {
	_ = st
	st.state = stateClosed
	delete(c.streams, st.id)
	if len(c.streams) == 0 && c.idleTimeout != 0 {
		c.idleTimer.Reset(c.idleTimeout)
	}
	c.wScheduler.CloseStream(st.id)
}

func (c *conn) close() {
	logger := c.lg
	logger.Info("closing connection")
	close(c.doneServing)
	if t := c.shutdownTimer; t != nil {
		t.Stop()
	}
	for _, st := range c.streams {
		c.closeStream(st)
	}
	_ = c.rwc.Close()
	c.cancelCtx()
	logger.Info("connection closed")
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

func (c *conn) goAway(isResponse bool) {
	c.serveG.Check()
	if c.inGoAway {
		return
	}
	c.inGoAway = true
	c.needToSendGoAway = true
	c.isGoAwayResponse = isResponse
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
	f    codec.Frame
	free func() // free should be called once the frame is no longer needed
	err  error
}

// frameWriteResult is the message passed from writeFrameAsync to the serve goroutine.
type frameWriteResult struct {
	wr  frameWriteRequest // what was written (or attempted)
	err error             // result of the writeFrame call
}
