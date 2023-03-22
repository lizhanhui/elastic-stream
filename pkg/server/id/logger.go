package id

import (
	"context"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

type LogAble interface {
	Allocator
	Logger() *zap.Logger
}

// Logger is a wrapper of Allocator that logs all operations.
type Logger struct {
	LogAble
}

func (l Logger) Alloc(ctx context.Context) (id uint64, err error) {
	id, err = l.LogAble.Alloc(ctx)

	logger := l.logger()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("alloc id", zap.Uint64("id", id), zap.Error(err))
	}
	return
}

func (l Logger) AllocN(ctx context.Context, n int) (ids []uint64, err error) {
	ids, err = l.LogAble.AllocN(ctx, n)

	logger := l.logger()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("alloc ids", zap.Int("num", n), zap.Uint64s("ids", ids), zap.Error(err))
	}
	return
}

func (l Logger) Reset(ctx context.Context) (err error) {
	err = l.LogAble.Reset(ctx)

	logger := l.logger()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("reset allocator", zap.Error(err))
	}
	return
}

func (l Logger) logger() *zap.Logger {
	if l.LogAble.Logger() != nil {
		return l.LogAble.Logger()
	}
	return zap.NewNop()
}
