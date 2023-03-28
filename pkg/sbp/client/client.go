package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// Address is the address of a server, in the format of "host:port"
type Address = string

const (
	_defaultHeartbeatTimeout = 15 * time.Second
)

var (
	clientIDCounter = atomic.Int32{}
)

type Client interface {
	Do(req protocol.OutRequest, addr Address) (protocol.InResponse, error)
	SealRanges(req *protocol.SealRangesRequest, addr Address) (*protocol.SealRangesResponse, error)
}

// A SbpClient internally caches connections to servers.
// It is safe for concurrent use by multiple goroutines.
type SbpClient struct {
	// TODO move into a config
	// IdleConnTimeout is the maximum amount of time an idle (keep-alive) connection
	// will remain idle before closing itself.
	// If zero, no idle connections are closed.
	IdleConnTimeout time.Duration
	// ReadIdleTimeout is the timeout after which a health check using Heartbeat
	// frame will be carried out if no frame is received on the connection.
	// If zero, no health check is performed.
	ReadIdleTimeout time.Duration
	// HeartbeatTimeout is the timeout after which the connection will be closed
	// if a response to Heartbeat is not received.
	// Default to _defaultHeartbeatTimeout
	HeartbeatTimeout time.Duration
	// Format is the format of the frames sent to the server.
	// Default to format.FlatBuffer
	Format format.Format

	id       string
	connPool *connPool

	lg *zap.Logger
}

// NewClient creates a client
func NewClient(lg *zap.Logger) *SbpClient {
	c := &SbpClient{
		lg: lg,
	}
	c.id = newClientID()
	c.connPool = newConnPool(c)
	return c
}

// Do sends a request to the server and returns the response.
// The request is sent to the server specified by addr.
// On success, the response is returned. On error, the response is nil and the error is returned.
func (c *SbpClient) Do(req protocol.OutRequest, addr Address) (protocol.InResponse, error) {
	logger := c.lg.With(zap.String("address", addr))
	if req.Timeout() > 0 {
		ctx, cancel := context.WithTimeout(req.Context(), time.Duration(req.Timeout())*time.Millisecond)
		defer cancel()
		req.SetContext(ctx)
	}

	// TODO retry when error is retryable
	conn, err := c.connPool.getConn(req, addr)
	if err != nil {
		logger.Error("failed to get connection", zap.Error(err))
		return nil, errors.Wrapf(err, "get connection to %s", addr)
	}

	resp, err := conn.roundTrip(req)
	if err != nil {
		logger.Error("round trip failed", zap.Error(err))
	}

	return resp, err
}

func (c *SbpClient) SealRanges(req *protocol.SealRangesRequest, addr Address) (*protocol.SealRangesResponse, error) {
	resp, err := c.Do(req, addr)
	if err != nil {
		return nil, err
	}
	if sealResp, ok := resp.(*protocol.SealRangesResponse); ok {
		return sealResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

func (c *SbpClient) CloseIdleConnections() {
	logger := c.lg
	c.connPool.closeIdleConnections()
	logger.Info("close idle connections")
}

func (c *SbpClient) dialConn(ctx context.Context, addr string) (*conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s", addr)
	}
	return c.newConn(conn)
}

func (c *SbpClient) newConn(rwc net.Conn) (*conn, error) {
	logger := c.lg.With(zap.String("remote-server-addr", rwc.RemoteAddr().String()))
	cc := &conn{
		c:            c,
		conn:         rwc,
		readerDone:   make(chan struct{}),
		streams:      make(map[uint32]*stream),
		heartbeats:   make(map[uint32]chan struct{}),
		nextStreamID: 1,
		reqMu:        make(chan struct{}, 1),
		fr:           codec.NewFramer(bufio.NewWriter(rwc), bufio.NewReader(rwc), logger),
		lg:           logger,
	}
	if c.IdleConnTimeout > 0 {
		cc.idleTimeout = c.IdleConnTimeout
		cc.idleTimer = time.AfterFunc(c.IdleConnTimeout, cc.onIdleTimeout)
	}

	logger.Info("connection created")
	go cc.readLoop()
	return cc, nil
}

func (c *SbpClient) Logger() *zap.Logger {
	return c.lg
}

func (c *SbpClient) heartbeatTimeout() time.Duration {
	if c.HeartbeatTimeout > 0 {
		return c.HeartbeatTimeout
	}
	return _defaultHeartbeatTimeout
}

func (c *SbpClient) format() format.Format {
	if c.Format.Valid() {
		return c.Format
	}
	return format.FlatBuffer()
}

func newClientID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("pm|%s|%d|%d|%d", hostname, os.Getpid(), clientIDCounter.Add(1), time.Now().UnixNano())
}
