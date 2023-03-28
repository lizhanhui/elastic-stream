package client

import (
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

type LogAble interface {
	Client
	Logger() *zap.Logger
}
type Logger struct {
	LogAble
}

func (l Logger) Do(req protocol.OutRequest, addr Address) (protocol.InResponse, error) {
	resp, err := l.LogAble.Do(req, addr)

	logger := l.logger().With(zap.String("address", addr))
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Debug("do request", zap.Any("request", req), zap.Any("response", resp), zap.Error(err))
	}
	return resp, err
}

func (l Logger) logger() *zap.Logger {
	if l.LogAble.Logger() != nil {
		return l.LogAble.Logger()
	}
	return zap.NewNop()
}
