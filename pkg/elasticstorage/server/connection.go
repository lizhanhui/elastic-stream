package server

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/AutoMQ/placement-manager/pkg/elasticstorage/codec"
)

type Conn struct {
	// Immutable:
	srv  *Server
	conn net.Conn

	ctx         context.Context
	framer      *codec.Framer
	doneServing chan struct{}        // closed when serverConn.serve ends
	readFrameCh chan readFrameResult // written by serverConn.readFrames

	// Everything following is owned by the serve loop; use serveG.check():
	//serveG            goroutineLock // used to verify funcs are on serve()
	maxClientStreamID uint32 // max ever seen from client (odd), or 0 if there have been no client requests
	maxPushPromiseID  uint32 // ID of the last push promise (even), or 0 if there have been no pushes
	streams           map[uint32]*stream
	inGoAway          bool        // we've started to or sent GOAWAY
	needToSendGoAway  bool        // we need to schedule a GOAWAY frame write
	shutdownTimer     *time.Timer // nil until used
	idleTimer         *time.Timer // nil if unused

	// Used by startGracefulShutdown.
	shutdownOnce sync.Once
}

type readFrameResult struct {
	f   codec.Frame
	err error
}
