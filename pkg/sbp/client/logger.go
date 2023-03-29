package client

import (
	"github.com/google/uuid"
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
	logger := l.Logger()

	debug := logger.Core().Enabled(zap.DebugLevel)
	if debug {
		traceID, _ := uuid.NewRandom()
		logger = logger.With(zap.String("address", addr), zap.String("trace-id", traceID.String()))
		logger.Debug("do request", zap.Any("request", req))
	}

	resp, err := l.LogAble.Do(req, addr)

	if debug {
		logger.Debug("do request done", zap.Any("response", resp), zap.Error(err))
	}
	return resp, err
}

func (l Logger) logger() *zap.Logger {
	if l.LogAble.Logger() != nil {
		return l.LogAble.Logger()
	}
	return zap.NewNop()
}
