package client

import (
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

type Logger struct {
	Client
}

func (l *Logger) Do(req protocol.OutRequest, addr address) (protocol.InResponse, error) {
	resp, err := l.Client.Do(req, addr)

	logger := l.Client.lg.With(zap.String("address", addr))
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Debug("do request", zap.Any("request", req), zap.Any("response", resp), zap.Error(err))
	}
	return resp, err
}
