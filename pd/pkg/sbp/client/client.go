//go:generate mockgen -typed -source=client.go -destination=client_mock.go -package=client Client
package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/pkg/sbp/codec"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/server/config"
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
	Do(ctx context.Context, req protocol.OutRequest, addr Address) (protocol.InResponse, error)
	Shutdown(ctx context.Context)
	SealRange(ctx context.Context, req *protocol.SealRangeRequest, addr Address) (*protocol.SealRangeResponse, error)
	CreateRange(ctx context.Context, req *protocol.CreateRangeRequest, addr Address) (*protocol.CreateRangeResponse, error)
	CreateStream(ctx context.Context, req *protocol.CreateStreamRequest, addr Address) (*protocol.CreateStreamResponse, error)
	ReportMetrics(ctx context.Context, req *protocol.ReportMetricsRequest, addr Address) (*protocol.ReportMetricsResponse, error)
	CommitObject(ctx context.Context, req *protocol.CommitObjectRequest, addr Address) (*protocol.CommitObjectResponse, error)
}

// A SbpClient internally caches connections to servers.
// It is safe for concurrent use by multiple goroutines.
type SbpClient struct {
	// Format is the format of the frames sent to the server.
	// Default to format.FlatBuffer
	Format codec.Format
	cfg    *config.SbpClient

	id       string
	connPool *connPool

	lg *zap.Logger
}

// NewClient creates a client
func NewClient(cfg *config.SbpClient, lg *zap.Logger) Client {
	if cfg == nil {
		cfg = &config.SbpClient{}
	}
	c := &SbpClient{
		cfg: cfg,
		lg:  lg,
	}
	c.id = newClientID()
	c.connPool = newConnPool(c)
	return c
}

// Do sends a request to the server and returns the response.
// The request is sent to the server specified by addr.
// On success, the response is returned. On error, the response is nil and the error is returned.
func (c *SbpClient) Do(ctx context.Context, req protocol.OutRequest, addr Address) (protocol.InResponse, error) {
	logger := c.lg.With(zap.String("address", addr))

	debug := logger.Core().Enabled(zap.DebugLevel)
	if debug {
		traceID, _ := uuid.NewRandom()
		logger = logger.With(zap.String("trace-id", traceID.String()))
		logger.Debug("do request", zap.Any("request", req), zap.String("request-type", fmt.Sprintf("%T", req)))
	}

	if req.Timeout() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout())*time.Millisecond)
		defer cancel()
	}
	req.SetContext(ctx)

	// TODO retry when error is retryable
	cc, err := c.connPool.getConn(req, addr)
	if err != nil {
		logger.Error("failed to get connection", zap.Error(err))
		return nil, errors.WithMessagef(err, "get connection to %s", addr)
	}

	resp, err := cc.roundTrip(req)
	if err != nil {
		logger.Error("round trip failed", zap.Error(err))
	}

	if debug {
		logger.Debug("do request done", zap.Any("response", resp), zap.String("response-type", fmt.Sprintf("%T", resp)), zap.Error(err))
	}

	return resp, err
}

func (c *SbpClient) SealRange(ctx context.Context, req *protocol.SealRangeRequest, addr Address) (*protocol.SealRangeResponse, error) {
	resp, err := c.Do(ctx, req, addr)
	if err != nil {
		return nil, err
	}
	if sealResp, ok := resp.(*protocol.SealRangeResponse); ok {
		return sealResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

func (c *SbpClient) CreateRange(ctx context.Context, req *protocol.CreateRangeRequest, addr Address) (*protocol.CreateRangeResponse, error) {
	resp, err := c.Do(ctx, req, addr)
	if err != nil {
		return nil, err
	}
	if createResp, ok := resp.(*protocol.CreateRangeResponse); ok {
		return createResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

func (c *SbpClient) CreateStream(ctx context.Context, req *protocol.CreateStreamRequest, addr Address) (*protocol.CreateStreamResponse, error) {
	resp, err := c.Do(ctx, req, addr)
	if err != nil {
		return nil, err
	}
	if createResp, ok := resp.(*protocol.CreateStreamResponse); ok {
		return createResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

func (c *SbpClient) ReportMetrics(ctx context.Context, req *protocol.ReportMetricsRequest, addr Address) (*protocol.ReportMetricsResponse, error) {
	resp, err := c.Do(ctx, req, addr)
	if err != nil {
		return nil, err
	}
	if reportResp, ok := resp.(*protocol.ReportMetricsResponse); ok {
		return reportResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

func (c *SbpClient) CommitObject(ctx context.Context, req *protocol.CommitObjectRequest, addr Address) (*protocol.CommitObjectResponse, error) {
	resp, err := c.Do(ctx, req, addr)
	if err != nil {
		return nil, err
	}
	if commitResp, ok := resp.(*protocol.CommitObjectResponse); ok {
		return commitResp, nil
	}
	if sysErr, ok := resp.(*protocol.SystemErrorResponse); ok {
		return nil, errors.Errorf("system error, code: %s, message: %s", sysErr.Status.Code, sysErr.Status.Message)
	}
	return nil, errors.Errorf("sbp: unexpected response type %T", resp)
}

// CloseIdleConnections closes any connections which were previously
// connected from previous requests but are now sitting idle.
// It does not interrupt any connections currently in use.
func (c *SbpClient) CloseIdleConnections() {
	logger := c.lg
	c.connPool.closeIdleConnections()
	logger.Info("close idle connections")
}

// Shutdown gracefully closes all connections, waits for all pending requests to complete.
func (c *SbpClient) Shutdown(ctx context.Context) {
	logger := c.lg
	c.connPool.closeAllConnections(ctx)
	logger.Info("close all connections")
}

func (c *SbpClient) dialConn(ctx context.Context, addr string) (*conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.WithMessagef(err, "dial %s", addr)
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
	cc.cond = sync.NewCond(&cc.mu)
	if c.cfg.IdleConnTimeout > 0 {
		cc.idleTimeout = c.cfg.IdleConnTimeout
		cc.idleTimer = time.AfterFunc(c.cfg.IdleConnTimeout, cc.onIdleTimeout)
	}

	logger.Info("connection created")
	go cc.readLoop()
	return cc, nil
}

func (c *SbpClient) Logger() *zap.Logger {
	return c.lg
}

func (c *SbpClient) heartbeatTimeout() time.Duration {
	if c.cfg.HeartbeatTimeout > 0 {
		return c.cfg.HeartbeatTimeout
	}
	return _defaultHeartbeatTimeout
}

func (c *SbpClient) format() codec.Format {
	if _, ok := codec.EnumNamesFormat[c.Format]; ok {
		return c.Format
	}
	return codec.DefaultFormat()
}

func newClientID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("pd|%s|%d|%d|%d", hostname, os.Getpid(), clientIDCounter.Add(1), time.Now().UnixNano())
}
