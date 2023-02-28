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

	ctx         context.Context
	cancelCtx   context.CancelFunc
	framer      *codec.Framer
	doneServing chan struct{}        // closed when serverConn.serve ends
	readFrameCh chan readFrameResult // written by serverConn.readFrames

	// Everything following is owned by the serve loop; use serveG.Check():
	serveG            tphttp2.GoroutineLock // used to verify funcs are on serve()
	maxClientStreamID uint32                // max ever seen from client (odd), or 0 if there have been no client requests
	streams           map[uint32]*stream
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

	// TODO
}

func (c *conn) close() {
	c.server.trackConn(c, false)
	close(c.doneServing)
	if t := c.shutdownTimer; t != nil {
		t.Stop()
	}
	// TODO close streams
	_ = c.rwc.Close()
	c.cancelCtx()
	// TODO deal with panics
}

func (c *conn) startGracefulShutdown() {
	// TODO
}

type readFrameResult struct {
	f   codec.Frame
	err error
}
