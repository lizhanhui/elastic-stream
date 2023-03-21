package kv

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

type LogAble interface {
	KV
	Logger() *zap.Logger
}

// Logger is a wrapper of KV that logs all operations.
type Logger struct {
	LogAble
}

func (l Logger) Get(ctx context.Context, k []byte) (v []byte, err error) {
	logger := l.logger()
	v, err = l.LogAble.Get(ctx, k)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("kv get", zap.ByteString("key", k), zap.Binary("value", v), zap.Error(err))
	}
	return
}

func (l Logger) GetByRange(ctx context.Context, r Range, limit int64) (kvs []KeyValue, err error) {
	logger := l.logger()
	kvs, err = l.LogAble.GetByRange(ctx, r, limit)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		fields := []zap.Field{
			zap.ByteString("start-key", r.StartKey),
			zap.ByteString("end-key", r.EndKey),
			zap.Int64("limit", limit),
			zap.Error(err),
		}
		for i, kv := range kvs {
			fields = append(fields, zap.ByteString(fmt.Sprintf("key-%d", i), kv.Key), zap.Binary(fmt.Sprintf("value-%d", i), kv.Value))
		}
		logger.Debug("kv get by range", fields...)
	}
	return
}

func (l Logger) Put(ctx context.Context, k, v []byte, prevKV bool) (prevV []byte, err error) {
	logger := l.logger()
	prevV, err = l.LogAble.Put(ctx, k, v, prevKV)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("kv put", zap.ByteString("key", k), zap.Binary("value", v), zap.Bool("prev-kv", prevKV), zap.Binary("prev-value", prevV), zap.Error(err))
	}
	return
}

func (l Logger) BatchPut(ctx context.Context, kvs []KeyValue, prevKV bool) (prevKvs []KeyValue, err error) {
	logger := l.logger()
	prevKvs, err = l.LogAble.BatchPut(ctx, kvs, prevKV)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		fields := []zap.Field{
			zap.Bool("prev-kv", prevKV),
			zap.Error(err),
		}
		for i, kv := range kvs {
			fields = append(fields, zap.ByteString(fmt.Sprintf("key-%d", i), kv.Key), zap.Binary(fmt.Sprintf("value-%d", i), kv.Value))
		}
		for i, kv := range prevKvs {
			fields = append(fields, zap.ByteString(fmt.Sprintf("prev-key-%d", i), kv.Key), zap.Binary(fmt.Sprintf("prev-value-%d", i), kv.Value))
		}
		logger.Debug("kv batch put", fields...)
	}
	return
}

func (l Logger) Delete(ctx context.Context, k []byte, prevKV bool) (prevV []byte, err error) {
	logger := l.logger()
	prevV, err = l.LogAble.Delete(ctx, k, prevKV)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		logger.Debug("kv delete", zap.ByteString("key", k), zap.Bool("prev-kv", prevKV), zap.Binary("prev-value", prevV), zap.Error(err))
	}
	return
}

func (l Logger) BatchDelete(ctx context.Context, ks [][]byte, prevKV bool) (prevKvs []KeyValue, err error) {
	logger := l.logger()
	prevKvs, err = l.LogAble.BatchDelete(ctx, ks, prevKV)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger = logger.With(traceutil.TraceLogField(ctx))
		fields := []zap.Field{
			zap.Bool("prev-kv", prevKV),
			zap.Error(err),
		}
		for i, k := range ks {
			fields = append(fields, zap.ByteString(fmt.Sprintf("key-%d", i), k))
		}
		for i, kv := range prevKvs {
			fields = append(fields, zap.ByteString(fmt.Sprintf("prev-key-%d", i), kv.Key), zap.Binary(fmt.Sprintf("prev-value-%d", i), kv.Value))
		}
		logger.Debug("kv batch delete", fields...)
	}
	return
}

func (l Logger) logger() *zap.Logger {
	if l.LogAble.Logger() != nil {
		return l.LogAble.Logger()
	}
	return zap.NewNop()
}
