package endpoint

import (
	"context"
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/fbutil"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

const (
	_streamIDFormat = _int64Format
	_streamIDLen    = _int64Len

	_streamPath   = "streams"
	_streamPrefix = _streamPath + kv.KeySeparator
	_streamFormat = _streamPath + kv.KeySeparator + _streamIDFormat // max length of int64 is 20
	_streamKeyLen = len(_streamPath) + len(kv.KeySeparator) + _streamIDLen

	_streamByRangeLimit = 1e4
)

// StreamEndpoint defines operations on stream.
type StreamEndpoint interface {
	// CreateStream creates a new stream based on the given stream and returns it.
	CreateStream(ctx context.Context, stream *rpcfb.StreamT) (*rpcfb.StreamT, error)
	// DeleteStream deletes the stream with the given stream id and returns it.
	DeleteStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error)
	// UpdateStream updates the stream with the given stream and returns it.
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the new epoch is less than the old one.
	UpdateStream(ctx context.Context, param *model.UpdateStreamParam) (*rpcfb.StreamT, error)
	// GetStream gets the stream with the given stream id.
	GetStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error)
	// ForEachStream calls the given function for every stream in the storage.
	// If f returns an error, the iteration is stopped and the error is returned.
	ForEachStream(ctx context.Context, f func(stream *rpcfb.StreamT) error) error
}

func (e *Endpoint) CreateStream(ctx context.Context, stream *rpcfb.StreamT) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", stream.StreamId), traceutil.TraceLogField(ctx))

	key := streamPath(stream.StreamId)
	value := fbutil.Marshal(stream)
	prevValue, err := e.KV.Put(ctx, key, value, true)
	mcache.Free(value)

	if err != nil {
		logger.Error("failed to save stream", zap.Error(err))
		return nil, errors.Wrapf(err, "save stream %d", stream.StreamId)
	}
	if prevValue != nil {
		logger.Warn("stream already exist, will override it")
	}

	return stream, nil
}

func (e *Endpoint) DeleteStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	prevV, err := e.KV.Delete(ctx, streamPath(streamID), true)
	if err != nil {
		logger.Error("failed to delete stream", zap.Error(err))
		return nil, errors.Wrapf(err, "delete stream %d", streamID)
	}
	if prevV == nil {
		logger.Warn("stream not found when delete stream")
		return nil, nil
	}
	// TODO: delete ranges asynchronously
	rangeInStreamPrefix := []byte(fmt.Sprintf(_rangeStreamPrefixFormat, streamID))
	_, _ = e.KV.DeleteByPrefixes(ctx, [][]byte{rangeInStreamPrefix})
	// TODO: delete index asynchronously

	return rpcfb.GetRootAsStream(prevV, 0).UnPack(), nil
}

func (e *Endpoint) UpdateStream(ctx context.Context, param *model.UpdateStreamParam) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", param.StreamID), traceutil.TraceLogField(ctx))

	var newStream *rpcfb.StreamT
	err := e.KV.ExecInTxn(ctx, func(kv kv.BasicKV) error {
		key := streamPath(param.StreamID)

		v, err := kv.Get(ctx, key)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.Wrapf(err, "get stream %d", param.StreamID)
		}
		if v == nil {
			logger.Error("stream not found when update stream")
			return errors.Wrapf(model.ErrStreamNotFound, "stream %d", param.StreamID)
		}

		oldStream := rpcfb.GetRootAsStream(v, 0).UnPack()
		// Incremental Update
		if param.Replica > 0 {
			oldStream.Replica = param.Replica
		}
		if param.AckCount > 0 {
			oldStream.AckCount = param.AckCount
		}
		if param.RetentionPeriodMs >= 0 {
			oldStream.RetentionPeriodMs = param.RetentionPeriodMs
		}
		if param.Epoch >= 0 {
			if param.Epoch < oldStream.Epoch {
				logger.Error("invalid epoch", zap.Int64("new-epoch", param.Epoch), zap.Int64("old-epoch", oldStream.Epoch))
				return errors.Wrapf(model.ErrInvalidStreamEpoch, "new epoch %d < old epoch %d", param.Epoch, oldStream.Epoch)
			}
			oldStream.Epoch = param.Epoch
		}
		newStream = oldStream

		streamInfo := fbutil.Marshal(newStream)
		_, _ = kv.Put(ctx, key, streamInfo, false)
		mcache.Free(streamInfo)

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "update stream %d", param.StreamID)
	}

	return newStream, nil
}

func (e *Endpoint) GetStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	v, err := e.KV.Get(ctx, streamPath(streamID))
	if err != nil {
		logger.Error("failed to get stream", zap.Error(err))
		return nil, errors.Wrapf(err, "get stream %d", streamID)
	}
	if v == nil {
		logger.Warn("stream not found")
		return nil, nil
	}

	return rpcfb.GetRootAsStream(v, 0).UnPack(), nil
}

func (e *Endpoint) ForEachStream(ctx context.Context, f func(stream *rpcfb.StreamT) error) error {
	var startID = model.MinStreamID
	for startID >= model.MinStreamID {
		nextID, err := e.forEachStreamLimited(ctx, f, startID, _streamByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachStreamLimited(ctx context.Context, f func(stream *rpcfb.StreamT) error, startID int64, limit int64) (nextID int64, err error) {
	logger := e.lg.With(traceutil.TraceLogField(ctx))

	startKey := streamPath(startID)
	kvs, _, more, err := e.KV.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endStreamPath()}, 0, limit, false)
	if err != nil {
		logger.Error("failed to get streams", zap.Int64("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return model.MinStreamID - 1, errors.Wrap(err, "get streams")
	}

	for _, streamKV := range kvs {
		stream := rpcfb.GetRootAsStream(streamKV.Value, 0).UnPack()
		nextID = stream.StreamId + 1
		err = f(stream)
		if err != nil {
			return model.MinStreamID - 1, err
		}
	}

	if !more {
		// no more streams
		nextID = model.MinStreamID - 1
	}
	return
}

func (e *Endpoint) endStreamPath() []byte {
	return e.KV.GetPrefixRangeEnd([]byte(_streamPrefix))
}

func streamPath(streamID int64) []byte {
	res := make([]byte, 0, _streamKeyLen)
	res = fmt.Appendf(res, _streamFormat, streamID)
	return res
}

//nolint:unused
func streamIDFromPath(path []byte) (streamID int64, err error) {
	_, err = fmt.Sscanf(string(path), _streamFormat, &streamID)
	if err != nil {
		err = errors.Wrapf(err, "parse stream id from path %s", string(path))
	}
	return
}
