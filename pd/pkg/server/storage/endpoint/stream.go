package endpoint

import (
	"context"
	"fmt"
	"time"

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
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the epoch mismatches.
	// NOTE: It's OK to delete a deleted stream.
	DeleteStream(ctx context.Context, param *model.DeleteStreamParam, delay time.Duration) (*rpcfb.StreamT, error)
	// UpdateStream updates the stream with the given stream and returns it.
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the new epoch is less than the old one.
	UpdateStream(ctx context.Context, param *model.UpdateStreamParam) (*rpcfb.StreamT, error)
	// TrimStream trims the stream with the given start offset and returns the stream and the first range after trimming.
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the epoch mismatches.
	// It returns model.ErrInvalidStreamOffset if the offset is less than the stream start offset.
	// It returns model.ErrRangeNotFound if there is no range covering the start offset.
	TrimStream(ctx context.Context, param *model.TrimStreamParam) (*rpcfb.StreamT, *rpcfb.RangeT, error)
	// GetStream gets the stream with the given stream id.
	GetStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error)
	// ForEachStream calls the given function for every stream in the storage.
	// If f returns an error, the iteration is stopped and the error is returned.
	// NOTE: Deleted streams are also iterated.
	ForEachStream(ctx context.Context, f func(stream *rpcfb.StreamT) error) error
}

func (e *Endpoint) CreateStream(ctx context.Context, stream *rpcfb.StreamT) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", stream.StreamId), traceutil.TraceLogField(ctx))

	key := streamPath(stream.StreamId)
	value := fbutil.Marshal(stream)
	prevValue, err := e.KV.Put(ctx, key, value, true, 0)
	mcache.Free(value)

	if err != nil {
		logger.Error("failed to save stream", zap.Error(err))
		return nil, errors.WithMessagef(err, "save stream %d", stream.StreamId)
	}
	if prevValue != nil {
		logger.Warn("stream already exist, will override it")
	}

	return stream, nil
}

func (e *Endpoint) DeleteStream(ctx context.Context, p *model.DeleteStreamParam, delay time.Duration) (*rpcfb.StreamT, error) {
	logger := e.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	var deletedStream *rpcfb.StreamT
	err := e.KV.ExecInTxn(ctx, func(basicKV kv.BasicKV) error {
		k := streamPath(p.StreamID)
		v, err := basicKV.Get(ctx, k)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.WithMessagef(err, "get stream %d", p.StreamID)
		}
		if v == nil {
			logger.Error("stream not found")
			return errors.WithMessagef(model.ErrStreamNotFound, "stream %d", p.StreamID)
		}

		s := rpcfb.GetRootAsStream(v, 0).UnPack()
		deletedStream = s
		if s.Deleted {
			logger.Warn("stream already deleted")
			return nil
		}
		if p.Epoch != s.Epoch {
			logger.Error("epoch mismatch")
			return errors.Wrapf(model.ErrInvalidStreamEpoch, "stream %d epoch %d != %d", p.StreamID, p.Epoch, s.Epoch)
		}
		s.Deleted = true

		streamInfo := fbutil.Marshal(s)
		_, _ = basicKV.Put(ctx, k, streamInfo, true, int64(delay.Seconds()))
		mcache.Free(streamInfo)

		// Delete ranges in the stream.
		_, _ = basicKV.DeleteByRange(ctx, kv.Range{
			StartKey: rangePathInSteam(p.StreamID, model.MinRangeIndex),
			EndKey:   e.endRangePathInStream(p.StreamID),
		})

		return nil
	})
	if err != nil {
		return nil, errors.WithMessagef(err, "delete stream %d", p.StreamID)
	}

	// TODO: delete index asynchronously

	return deletedStream, nil
}

func (e *Endpoint) TrimStream(ctx context.Context, p *model.TrimStreamParam) (*rpcfb.StreamT, *rpcfb.RangeT, error) {
	logger := e.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	firstRange, err := e.getRangeByOffset(ctx, p.StreamID, p.StartOffset)
	if err != nil {
		logger.Error("failed to get range by offset", zap.Error(err))
		return nil, nil, errors.WithMessagef(err, "get range by offset %d", p.StartOffset)
	}

	var trimmedStream *rpcfb.StreamT
	var trimmedRange *rpcfb.RangeT
	err = e.KV.ExecInTxn(ctx, func(basicKV kv.BasicKV) error {
		// get, check and update the stream
		sk := streamPath(p.StreamID)
		sv, err := basicKV.Get(ctx, sk)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.WithMessagef(err, "get stream %d", p.StreamID)
		}
		if sv == nil {
			logger.Error("stream not found")
			return errors.WithMessagef(model.ErrStreamNotFound, "stream %d", p.StreamID)
		}
		s := rpcfb.GetRootAsStream(sv, 0).UnPack()
		if s.Deleted {
			logger.Error("stream already deleted")
			return errors.WithMessagef(model.ErrStreamNotFound, "stream %d deleted", p.StreamID)
		}
		if p.Epoch != s.Epoch {
			logger.Error("invalid stream epoch", zap.Int64("stream-epoch", s.Epoch))
			return errors.WithMessagef(model.ErrInvalidStreamEpoch, "stream %d epoch %d != %d", p.StreamID, p.Epoch, s.Epoch)
		}
		if p.StartOffset < s.StartOffset {
			// As we have check the range before, this should never happen.
			logger.Error("invalid start offset", zap.Int64("stream-start-offset", s.StartOffset))
			return errors.WithMessagef(model.ErrInvalidStreamOffset, "stream %d start offset %d < %d", p.StreamID, p.StartOffset, s.StartOffset)
		}
		if firstRange == nil {
			logger.Error("range not found")
			return errors.WithMessagef(model.ErrRangeNotFound, "invalid offset: range not found at offset %d", p.StartOffset)
		}
		s.StartOffset = p.StartOffset
		trimmedStream = s

		streamInfo := fbutil.Marshal(s)
		_, _ = basicKV.Put(ctx, sk, streamInfo, true, 0)
		mcache.Free(streamInfo)

		// update the range
		rk := rangePathInSteam(p.StreamID, firstRange.Index)
		rv, err := basicKV.Get(ctx, rk)
		if err != nil {
			logger.Error("failed to get range", zap.Error(err))
			return errors.WithMessagef(err, "get range %d in stream %d", firstRange.Index, p.StreamID)
		}
		if rv == nil {
			logger.Error("range not found")
			return errors.WithMessagef(model.ErrRangeNotFound, "range %d in stream %d", firstRange.Index, p.StreamID)
		}
		r := rpcfb.GetRootAsRange(rv, 0).UnPack()
		r.Start = p.StartOffset
		trimmedRange = r

		rangeInfo := fbutil.Marshal(r)
		_, _ = basicKV.Put(ctx, rk, rangeInfo, true, 0)
		mcache.Free(rangeInfo)

		// delete ranges before the first range
		_, _ = basicKV.DeleteByRange(ctx, kv.Range{
			StartKey: rangePathInSteam(p.StreamID, model.MinRangeIndex),
			EndKey:   rangePathInSteam(p.StreamID, firstRange.Index),
		})

		return nil
	})
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "trim stream %d", p.StreamID)
	}

	return trimmedStream, trimmedRange, nil
}

func (e *Endpoint) UpdateStream(ctx context.Context, p *model.UpdateStreamParam) (*rpcfb.StreamT, error) {
	logger := e.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	var newStream *rpcfb.StreamT
	err := e.KV.ExecInTxn(ctx, func(kv kv.BasicKV) error {
		key := streamPath(p.StreamID)
		v, err := kv.Get(ctx, key)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.WithMessagef(err, "get stream %d", p.StreamID)
		}
		if v == nil {
			logger.Error("stream not found")
			return errors.WithMessagef(model.ErrStreamNotFound, "stream %d", p.StreamID)
		}

		oldStream := rpcfb.GetRootAsStream(v, 0).UnPack()
		if oldStream.Deleted {
			logger.Error("stream already deleted")
			return errors.WithMessagef(model.ErrStreamNotFound, "stream %d deleted", p.StreamID)
		}
		// Incremental Update
		if p.Replica > 0 {
			oldStream.Replica = p.Replica
		}
		if p.AckCount > 0 {
			oldStream.AckCount = p.AckCount
		}
		if p.RetentionPeriodMs >= 0 {
			oldStream.RetentionPeriodMs = p.RetentionPeriodMs
		}
		if p.Epoch >= 0 {
			if p.Epoch < oldStream.Epoch {
				logger.Error("invalid epoch", zap.Int64("new-epoch", p.Epoch), zap.Int64("old-epoch", oldStream.Epoch))
				return errors.WithMessagef(model.ErrInvalidStreamEpoch, "new epoch %d < old epoch %d", p.Epoch, oldStream.Epoch)
			}
			oldStream.Epoch = p.Epoch
		}
		newStream = oldStream

		streamInfo := fbutil.Marshal(newStream)
		_, _ = kv.Put(ctx, key, streamInfo, false, 0)
		mcache.Free(streamInfo)

		return nil
	})
	if err != nil {
		return nil, errors.WithMessagef(err, "update stream %d", p.StreamID)
	}

	return newStream, nil
}

func (e *Endpoint) GetStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	v, err := e.KV.Get(ctx, streamPath(streamID))
	if err != nil {
		logger.Error("failed to get stream", zap.Error(err))
		return nil, errors.WithMessagef(err, "get stream %d", streamID)
	}
	if v == nil {
		logger.Warn("stream not found")
		return nil, nil
	}

	s := rpcfb.GetRootAsStream(v, 0).UnPack()
	if s.Deleted {
		logger.Warn("stream already deleted")
		return nil, nil
	}

	return s, nil
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
		return model.MinStreamID - 1, errors.WithMessage(err, "get streams")
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
		err = errors.WithMessagef(err, "parse stream id from path %s", string(path))
	}
	return
}
