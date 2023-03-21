package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	sbpServer "github.com/AutoMQ/placement-manager/pkg/sbp/server"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

type LogAble interface {
	sbpServer.Handler
	Logger() *zap.Logger
}

// SbpLogger is a wrapper of sbpServer.Handler that logs the request and response.
type SbpLogger struct {
	LogAble
}

func (l SbpLogger) ListRange(ctx context.Context, req *protocol.ListRangesRequest, resp *protocol.ListRangesResponse) {
	l.LogAble.ListRange(ctx, req, resp)
	l.LogIt(ctx, req, resp)
}

func (l SbpLogger) CreateStreams(ctx context.Context, req *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse) {
	l.LogAble.CreateStreams(ctx, req, resp)
	l.LogIt(ctx, req, resp)
}

func (l SbpLogger) DeleteStreams(ctx context.Context, req *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse) {
	l.LogAble.DeleteStreams(ctx, req, resp)
	l.LogIt(ctx, req, resp)
}

func (l SbpLogger) UpdateStreams(ctx context.Context, req *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse) {
	l.LogAble.UpdateStreams(ctx, req, resp)
	l.LogIt(ctx, req, resp)
}

func (l SbpLogger) DescribeStreams(ctx context.Context, req *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse) {
	l.LogAble.DescribeStreams(ctx, req, resp)
	l.LogIt(ctx, req, resp)
}

func (l SbpLogger) LogIt(ctx context.Context, req protocol.Request, resp protocol.Response) {
	logger := l.logger()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("handler log", zap.Any("request", req), zap.Any("response", resp))
	}
}

func (l SbpLogger) logger() *zap.Logger {
	if l.LogAble.Logger() != nil {
		return l.LogAble.Logger()
	}
	return zap.NewNop()
}
