package endpoint

import (
	"context"
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/kv"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

const (
	_rangeIDFormat    = _int32Format
	_rangeIDLen       = _int32Len
	_dataNodeIDFormat = _int32Format
	_dataNodeIDLen    = _int32Len

	// ranges in stream
	_rangeStreamPath         = "range"
	_rangeStreamPrefix       = "s"
	_rangeStreamPrefixFormat = _rangeStreamPrefix + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeStreamPath + kv.KeySeparator
	_rangeStreamFormat       = _rangeStreamPrefix + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeStreamPath + kv.KeySeparator + _rangeIDFormat
	_rangeStreamKeyLen       = len(_rangeStreamPrefix) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + len(_rangeStreamPath) + len(kv.KeySeparator) + _rangeIDLen

	// ranges on data node
	_rangeNodePath               = "stream-range"
	_rangeNodePrefix             = "n"
	_rangeNodePrefixFormat       = _rangeNodePrefix + kv.KeySeparator + _dataNodeIDFormat + kv.KeySeparator + _rangeNodePath + kv.KeySeparator
	_rangeNodeStreamPrefixFormat = _rangeNodePrefix + kv.KeySeparator + _dataNodeIDFormat + kv.KeySeparator + _rangeNodePath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator
	_rangeNodeFormat             = _rangeNodePrefix + kv.KeySeparator + _dataNodeIDFormat + kv.KeySeparator + _rangeNodePath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat
	_rangeNodeKeyLen             = len(_rangeNodePrefix) + len(kv.KeySeparator) + _dataNodeIDLen + len(kv.KeySeparator) + len(_rangeNodePath) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + _rangeIDLen

	_rangeByRangeLimit = 1e4
)

type Range interface {
	SealRange(ctx context.Context, sealedRange *rpcfb.RangeT, writableRange *rpcfb.RangeT) (*rpcfb.RangeT, error)
	GetRange(ctx context.Context, rangeID *rpcfb.RangeIdT) (*rpcfb.RangeT, error)
	GetLastRange(ctx context.Context, streamID int64) (*rpcfb.RangeT, error)
	GetRangesByStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error)
	ForEachRangeInStream(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error) error
	GetRangeIDsByDataNode(ctx context.Context, dataNodeID int32) ([]*rpcfb.RangeIdT, error)
	ForEachRangeIDOnDataNode(ctx context.Context, dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error) error
	GetRangeIDsByDataNodeAndStream(ctx context.Context, streamID int64, dataNodeID int32) ([]*rpcfb.RangeIdT, error)
}

func (e *Endpoint) SealRange(ctx context.Context, sealedRange *rpcfb.RangeT, writableRange *rpcfb.RangeT) (*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("sealed-stream-id", sealedRange.StreamId), zap.Int32("sealed-range-index", sealedRange.RangeIndex),
		zap.Int64("writable-stream-id", writableRange.StreamId), zap.Int32("writable-range-index", writableRange.RangeIndex), traceutil.TraceLogField(ctx))

	kvs := make([]kv.KeyValue, 2)
	kvs[0] = kv.KeyValue{Key: rangePathInSteam(sealedRange.StreamId, sealedRange.RangeIndex), Value: fbutil.Marshal(sealedRange)}
	kvs[1] = kv.KeyValue{Key: rangePathInSteam(writableRange.StreamId, writableRange.RangeIndex), Value: fbutil.Marshal(writableRange)}

	preKvs, err := e.BatchPut(ctx, kvs, true)
	mcache.Free(kvs[0].Value)
	mcache.Free(kvs[1].Value)
	if err != nil {
		logger.Error("failed to seal range", zap.Error(err))
		return nil, errors.Wrap(err, "seal range")
	}

	if len(preKvs) > 1 {
		logger.Warn("seal range: writable range already exists")
	}
	if len(preKvs) < 1 {
		logger.Warn("seal range: sealed range not exists")
	}

	return writableRange, nil
}

func (e *Endpoint) GetRange(ctx context.Context, rangeID *rpcfb.RangeIdT) (*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("stream-id", rangeID.StreamId), zap.Int32("range-index", rangeID.RangeIndex), traceutil.TraceLogField(ctx))

	key := rangePathInSteam(rangeID.StreamId, rangeID.RangeIndex)
	value, err := e.Get(ctx, key)
	if err != nil {
		logger.Error("failed to get range", zap.Error(err))
		return nil, errors.Wrap(err, "get range")
	}
	if value == nil {
		return nil, nil
	}

	return rpcfb.GetRootAsRange(value, 0).UnPack(), nil
}

func (e *Endpoint) GetLastRange(ctx context.Context, streamID int64) (*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	kvs, err := e.GetByRange(ctx, kv.Range{StartKey: rangePathInSteam(streamID, MinRangeIndex), EndKey: e.endRangePathInStream(streamID)}, 1, true)
	if len(kvs) < 1 || err != nil {
		logger.Error("failed to get last range", zap.Error(err))
		return nil, err
	}

	return rpcfb.GetRootAsRange(kvs[0].Value, 0).UnPack(), nil
}

// GetRangesByStream returns the ranges of the given stream.
func (e *Endpoint) GetRangesByStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	// TODO set capacity
	ranges := make([]*rpcfb.RangeT, 0)
	err := e.ForEachRangeInStream(ctx, streamID, func(r *rpcfb.RangeT) error {
		ranges = append(ranges, r)
		return nil
	})
	if err != nil {
		logger.Error("failed to get ranges", zap.Error(err))
		return nil, errors.Wrap(err, "get ranges")
	}

	return ranges, nil
}

// ForEachRangeInStream calls the given function f for each range in the stream.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachRangeInStream(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error) error {
	var startID = MinRangeIndex
	for startID >= MinRangeIndex {
		nextID, err := e.forEachRangeInStreamLimited(ctx, streamID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeInStreamLimited(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error, startID int32, limit int64) (nextID int32, err error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	startKey := rangePathInSteam(streamID, startID)
	kvs, err := e.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathInStream(streamID)}, limit, false)
	if err != nil {
		logger.Error("failed to get ranges", zap.Int32("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return MinRangeIndex - 1, errors.Wrap(err, "get ranges")
	}

	for _, rangeKV := range kvs {
		r := rpcfb.GetRootAsRange(rangeKV.Value, 0).UnPack()
		nextID = r.RangeIndex + 1
		err = f(r)
		if err != nil {
			return MinRangeIndex - 1, err
		}
	}

	if int64(len(kvs)) < limit {
		// no more ranges
		nextID = MinRangeIndex - 1
	}
	return
}

func (e *Endpoint) endRangePathInStream(streamID int64) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeStreamPrefixFormat, streamID)))
}

func rangePathInSteam(streamID int64, rangeIndex int32) []byte {
	res := make([]byte, 0, _rangeStreamKeyLen)
	res = fmt.Appendf(res, _rangeStreamFormat, streamID, rangeIndex)
	return res
}

func (e *Endpoint) GetRangeIDsByDataNode(ctx context.Context, dataNodeID int32) ([]*rpcfb.RangeIdT, error) {
	logger := e.lg.With(zap.Int32("data-node-id", dataNodeID), traceutil.TraceLogField(ctx))

	// TODO set capacity
	rangeIDs := make([]*rpcfb.RangeIdT, 0)
	err := e.ForEachRangeIDOnDataNode(ctx, dataNodeID, func(rangeID *rpcfb.RangeIdT) error {
		rangeIDs = append(rangeIDs, rangeID)
		return nil
	})
	if err != nil {
		logger.Error("failed to get range ids by data node", zap.Error(err))
		return nil, errors.Wrap(err, "get range ids by data node")
	}

	return rangeIDs, nil
}

// ForEachRangeIDOnDataNode calls the given function f for each range on the data node.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachRangeIDOnDataNode(ctx context.Context, dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error) error {
	startID := &rpcfb.RangeIdT{StreamId: MinStreamID, RangeIndex: MinRangeIndex}
	for startID != nil && startID.StreamId >= MinStreamID && startID.RangeIndex >= MinRangeIndex {
		nextID, err := e.forEachRangeIDOnDataNodeLimited(ctx, dataNodeID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeIDOnDataNodeLimited(ctx context.Context, dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error, startID *rpcfb.RangeIdT, limit int64) (nextID *rpcfb.RangeIdT, err error) {
	logger := e.lg.With(zap.Int32("data-node-id", dataNodeID), traceutil.TraceLogField(ctx))

	startKey := rangePathOnDataNode(dataNodeID, startID.StreamId, startID.RangeIndex)
	kvs, err := e.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathOnDataNode(dataNodeID)}, limit, false)
	if err != nil {
		logger.Error("failed to get range ids by data node", zap.Int64("start-stream-id", startID.StreamId), zap.Int32("start-range-index", startID.RangeIndex), zap.Int64("limit", limit), zap.Error(err))
		return nil, errors.Wrap(err, "get range ids by data node")
	}

	nextID = startID
	for _, rangeKV := range kvs {
		_, streamID, rangeIndex, err := rangeIDFromPath(rangeKV.Key)
		if err != nil {
			return nil, err
		}
		nextID.StreamId = streamID
		nextID.RangeIndex = rangeIndex + 1
		err = f(&rpcfb.RangeIdT{StreamId: streamID, RangeIndex: rangeIndex})
		if err != nil {
			return nil, err
		}
	}

	// return nil if no more ranges
	if int64(len(kvs)) < limit {
		nextID = nil
	}
	return
}

func (e *Endpoint) endRangePathOnDataNode(dataNodeID int32) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeNodePrefixFormat, dataNodeID)))
}

func (e *Endpoint) GetRangeIDsByDataNodeAndStream(ctx context.Context, streamID int64, dataNodeID int32) ([]*rpcfb.RangeIdT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), zap.Int32("data-node-id", dataNodeID), traceutil.TraceLogField(ctx))

	startKey := rangePathOnDataNode(dataNodeID, streamID, MinRangeIndex)
	kvs, err := e.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathOnDataNodeInStream(dataNodeID, streamID)}, 0, false)
	if err != nil {
		logger.Error("failed to get range ids by data node and stream", zap.Error(err))
		return nil, errors.Wrap(err, "get range ids by data node and stream")
	}

	rangeIDs := make([]*rpcfb.RangeIdT, 0, len(kvs))
	for _, rangeKV := range kvs {
		_, _, rangeIndex, err := rangeIDFromPath(rangeKV.Key)
		if err != nil {
			return nil, err
		}
		rangeIDs = append(rangeIDs, &rpcfb.RangeIdT{StreamId: streamID, RangeIndex: rangeIndex})
	}

	return rangeIDs, nil
}

func (e *Endpoint) endRangePathOnDataNodeInStream(dataNodeID int32, streamID int64) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeNodeStreamPrefixFormat, dataNodeID, streamID)))
}

func rangePathOnDataNode(dataNodeID int32, streamID int64, rangeIndex int32) []byte {
	res := make([]byte, 0, _rangeNodeKeyLen)
	res = fmt.Appendf(res, _rangeNodeFormat, dataNodeID, streamID, rangeIndex)
	return res
}

func rangeIDFromPath(path []byte) (dataNodeID int32, streamID int64, rangeIndex int32, err error) {
	_, err = fmt.Sscanf(string(path), _rangeNodeFormat, &dataNodeID, &streamID, &rangeIndex)
	if err != nil {
		err = errors.Wrapf(err, "parse range path: %s", string(path))
	}
	return
}