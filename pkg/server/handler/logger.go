package handler

import (
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	sbpServer "github.com/AutoMQ/placement-manager/pkg/sbp/server"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

type LogAble interface {
	sbpServer.Handler
	Logger() *zap.Logger
}

// Logger is a wrapper of sbpServer.Handler that logs the request and response.
type Logger struct {
	Handler LogAble
}

func (l Logger) Heartbeat(req *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse) {
	l.Handler.Heartbeat(req, resp)
}

func (l Logger) AllocateID(req *protocol.IDAllocationRequest, resp *protocol.IDAllocationResponse) {
	l.Handler.AllocateID(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) ListRanges(req *protocol.ListRangesRequest, resp *protocol.ListRangesResponse) {
	l.Handler.ListRanges(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) SealRanges(req *protocol.SealRangesRequest, resp *protocol.SealRangesResponse) {
	l.Handler.SealRanges(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) CreateStreams(req *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse) {
	l.Handler.CreateStreams(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) DeleteStreams(req *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse) {
	l.Handler.DeleteStreams(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) UpdateStreams(req *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse) {
	l.Handler.UpdateStreams(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) DescribeStreams(req *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse) {
	l.Handler.DescribeStreams(req, resp)
	l.LogIt(req, resp)
}

func (l Logger) LogIt(req protocol.InRequest, resp protocol.OutResponse) {
	logger := l.logger()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(req.Context()))
		logger.Debug("handler log", zap.Any("request", req), zap.Any("response", resp))
	}
}

func (l Logger) logger() *zap.Logger {
	if l.Handler.Logger() != nil {
		return l.Handler.Logger()
	}
	return zap.NewNop()
}
